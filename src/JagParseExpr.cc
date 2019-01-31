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

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <stack>
#include <vector>
#include <exception>
#include <stdio.h>
#include <strings.h>
#include <string.h>
#include <math.h>
#include <algorithm>
#include <regex>

#include <JagParseExpr.h>
#include <JagVector.h>
#include <JagTableUtil.h>
#include <JagParseParam.h>
#include <JagStrSplit.h>
#include <JagSession.h>
#include <JagRequest.h>
#include <utfcpp/utf8.h>
#include <JagLang.h>
#include <JagParser.h>
#include <JagGeom.h>
#include <JagRange.h>
#include <JagHashStrStr.h>
#include <JagTime.h>
#include <JagUtil.h>
#include <JagCGAL.h>

/*****************************************************************************************
** _opString: used in where for final result
** strResult: used in select col for final result
** cmode: 0 for string
** cmode: 1 for int
** cmode: 2 for float/double
** coltype: 0  func(a+b)  a and b are both constants or funcation-computed values already
** coltype: 1  func(a+b)  a is table column, b is constant
** coltype: 2  func(a+b)  a is table column, b is table column
** typeMode:  == ltmode == rtmode == cmode
** lcmode: is constant or not
** rcmode: is constant or not
** checkFuncValid returns:  -1: error case and false
** checkFuncValid returns:  0: false
** checkFuncValid returns:  1: true
** checkFuncValid returns:  2: no data avaailable and gets ignored in computing
** unary calc in _doCalculation()
****************************************************************************************/

//////////////////////////////////// ExprElementNode ////////////////////////////
// ctor
ExprElementNode::ExprElementNode()
{
}

ExprElementNode::~ExprElementNode() 
{ 
}
 

//////////////////////////////////// StringElementNode ////////////////////////////////
// ctor
StringElementNode::StringElementNode() 
{
	_isElement = true;
}

/***
StringElementNode::StringElementNode(const StringElementNode& n) 
{
	_rowHash = NULL;
}

StringElementNode& StringElementNode::operator=(const StringElementNode& n) 
{
}
***/

// ctor
StringElementNode::StringElementNode( BinaryExpressionBuilder *builder, const Jstr &name, 
									 const AbaxFixString &value, 
                       				  const JagParseAttribute &jpa, int tabnum, int typeMode )
{
    _name = makeLowerString(name); 
	_value = value; _jpa = jpa; _tabnum = tabnum; _typeMode = typeMode;
    _srid = _offset = _length = _sig = _nodenum = _begincol = _endcol = 0;
    _type = ""; 
	// _columns =  columns;
	_builder = builder;
	_isElement = true;
	// prt(("s2075 StringElementNode ctor _columns=[%s]\n", _columns.c_str() ));

	if ( _name.size() > 0 ) {
		_builder->_pparam->initColHash();
		if ( strchr(_name.c_str(), '.' ) ) {
			JagStrSplit sp( _name, '.' );
			int n = sp.length();
			_builder->_pparam->_colHash->addKeyValue( sp[n-1], "1" );
		} else {
			_builder->_pparam->_colHash->addKeyValue( _name, "1" );
		}
	}
}

// dtor
StringElementNode::~StringElementNode() 
{
	/***
	prt(( "s6726 StringElementNode dtor called. _name=[%s] _value=[%s] _columns=[%s]\n", 
		  _name.c_str(), _value.c_str(), _columns.c_str() ));
    ***/
}

void StringElementNode::print( int mode ) {
	prt(("name=\"%s\" value=\"%s\"", _name.c_str(), _value.c_str()));
}


// -1: error
// 1: OK
int StringElementNode::setWhereRange( const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
						const int keylen[], const int numKeys[], int numTabs, bool &hasValue, 
						JagMinMax *minmax, AbaxFixString &str, int &typeMode, int &tabnum ) 
{
	//prt(("s6112  StringElementNode::setWhereRange _name=[%s] \n", _name.c_str() ));
	tabnum = -1;
	str = "";
	int acqpos;
	if ( _name.length() > 0 ) {
		// must be column name
		typeMode = 0;
		if ( maps[_tabnum]->getValue(_name, acqpos) ) {
			tabnum = _tabnum;
			minmax[_tabnum].offset = _offset = attrs[_tabnum][acqpos].offset;
			minmax[_tabnum].length = _length = attrs[_tabnum][acqpos].length;
			minmax[_tabnum].sig = _sig = attrs[_tabnum][acqpos].sig;
			minmax[_tabnum].type = _type = attrs[_tabnum][acqpos].type;

			minmax[_tabnum].dbcol = attrs[_tabnum][acqpos].dbcol;
			minmax[_tabnum].objcol = attrs[_tabnum][acqpos].objcol;
			minmax[_tabnum].colname = attrs[_tabnum][acqpos].colname;
			_begincol = attrs[_tabnum][acqpos].begincol;
			_endcol = attrs[_tabnum][acqpos].endcol;
			_srid = attrs[_tabnum][acqpos].srid;
			if ( !attrs[_tabnum][acqpos].isKey ) hasValue = 1;
		} else {
			//prt(("s6934 _name=[%s] return -1\n", _name.c_str() ));
			return -1;
		}
	} else {
		// const, direct return true
		str = _value;
		typeMode = _typeMode;
	}
	
	//prt(("s2091 StringElementNode::setWhereRange return 1\n" ));
	return 1;
}

int StringElementNode::setFuncAttribute( const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
	int &constMode, int &typeMode, bool &isAggregate, Jstr &type, int &collen, int &siglen )
{	
	//prt(("s4029 StringElementNode::setFuncAttribute _name=[%s] _type=[%s]\n", _name.c_str(), _type.c_str() ));
	int acqpos;

	if ( _name.length() > 0 ) {
		// must be column name
		if ( maps[_tabnum]->getValue(_name, acqpos) ) {
			_offset = attrs[_tabnum][acqpos].offset;
			collen = _length = attrs[_tabnum][acqpos].length;
			siglen = _sig = attrs[_tabnum][acqpos].sig;
			type = _type = attrs[_tabnum][acqpos].type;
			_begincol = attrs[_tabnum][acqpos].begincol;
			_endcol = attrs[_tabnum][acqpos].endcol;
			_srid = attrs[_tabnum][acqpos].srid;

			#if 0
			prt(("s0394  _name=[%s] type=[%s] collen=%d _begincol=%d _endcol=%d _srid=%d\n",  
					_name.c_str(), _type.c_str(), collen, _begincol, _endcol, _srid ));
			#endif

		} else {
			return 0;
		}

		if ( isInteger(_type) ) typeMode = 1;
		else if ( _type == JAG_C_COL_TYPE_FLOAT || _type == JAG_C_COL_TYPE_DOUBLE ) typeMode = 2;
		else typeMode = 0;
		constMode = 1;
	} else {
		constMode = 0;
		typeMode = _typeMode;
		//prt(("s8013 typeMode=%d\n", typeMode ));
		if ( 0 == typeMode ) {
			// is string
			collen = _value.length();
			siglen = 0;
			type = _type = JAG_C_COL_TYPE_STR;
		} else {
			// is float
			typeMode = 2;
			collen = JAG_MAX_INT_LEN;
			siglen = JAG_MAX_SIG_LEN;
			type = _type = JAG_C_COL_TYPE_FLOAT;
		}
	}
	
	return 1;
}

// method to set node number and split original command to separate aggregate funcs for use
int StringElementNode::getFuncAggregate( JagVector<Jstr> &selectParts, JagVector<int> &selectPartsOpcode, 
	JagVector<int> &selColSetAggParts, JagHashMap<AbaxInt, AbaxInt> &selColToselParts, int &nodenum, int treenum )
{
	_nodenum = nodenum++;
	return 1;
}

// method to split and format aggregate parts
int StringElementNode::getAggregateParts( Jstr &parts, int &nodenum )
{
	if ( _name.length() ) parts = _name;
	else parts = _value.c_str();
	_nodenum = nodenum++;
	return 1;
}
int StringElementNode::setAggregateValue( int nodenum, const char *buf, int length ) 
{ 
	return 1;
}

// return 1: get string to compare; 
// return 2: no string provided, regard as true
int StringElementNode::checkFuncValid(JagMergeReaderBase *ntr, const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[],
					const char *buffers[], AbaxFixString &str, int &typeMode, 
					Jstr &type, int &length, bool &first, bool useZero, bool setGlobal ) 
{
	prt(( "s3039 StEleNode::checkFuncValid _name=[%s] _value=[%s] _type=[%s]\n", _name.c_str(), _value.c_str(), _type.c_str() ));

	if ( _name.length() > 0 ) {
		if (  ( buffers[_tabnum] == NULL || buffers[_tabnum][0] == '\0' ) ) {
			// this buffers does not have value, regard as true
			typeMode = 0;
			type = _type;
			length = _length;
			//prt(("s0391 tmp9999 checkFuncValid buffers empty _tabnum=%d return 2\n", _tabnum ));
			return 2;
		}

		// _srid=_name=_type
		Jstr colobjstr = Jstr("OJAG=") + intToStr( _srid ) + "=" + _name + "=" + _type;
		//prt(("s0393 tmp9999 colobjstr=[%s]\n", colobjstr.c_str() ));
		// get str
		//prt(("s7012 _type=[%s]\n", _type.c_str() ));
		if ( JagParser::isPolyType( _type ) ) {
			//prt(("s3770 isPolyType getPolyDataString  ...\n" ));
			getPolyDataString( ntr, _type, maps, attrs, buffers, str );
		} else if ( JagParser::isGeoType( _type ) ) {
			//colobjstr += "=0";
			int dim = JagGeo::getDimension(_type);
			if ( 2 == dim ) {
				colobjstr += "=d 0:0:0:0";
			} else {
				colobjstr += "=d 0:0:0:0:0:0";
			}
			makeDataString( attrs, buffers, colobjstr, str );
		} else if (  _type == JAG_C_COL_TYPE_RANGE  ) {
			makeRangeDataString( attrs, buffers, colobjstr, str );
		} else {
			str = AbaxFixString(buffers[_tabnum]+_offset, _length);
			//prt(("s0233 AbaxFixString str=[%s] _type=[%s]\n", str.c_str(), _type.c_str() ));
		}

		if ( isInteger(_type) ) typeMode = 1;
		else if ( _type == JAG_C_COL_TYPE_FLOAT || _type == JAG_C_COL_TYPE_DOUBLE ) typeMode = 2;
		else if ( JagParser::isGeoType( _type ) ) typeMode = 2;
		else typeMode = 0;

		// prt(("s0396 _name=[%s] typeMode=%d\n", _name.c_str(), typeMode ));

		type = _type;
		length = _length;
	} else {
		str = _value;
		typeMode = _typeMode;
		type = "";
		length = 0;
	}

	// prt(("s0332 StringElementNode::checkfuncvalid return 1  got str=[%s]\n", str.c_str() ));
	return 1;
}

int StringElementNode::checkFuncValidConstantOnly( AbaxFixString &str, int &typeMode, Jstr &type, int &length )
{
	prt(("s1026 StringElementNode::checkFuncValidConstantOnly() ...\n" ));
	if ( _value.length() > 0 ) {
		str = _value;
		typeMode = _typeMode;
		type = "";
		length = 0;
	}

	prt(("s1120 str=_value=[%s] typeMode=%d\n", str.c_str(), typeMode ));
	return 1;
}

#if 0
// return str:  "OJAG=srid=tab=TYPE bbx  x y z"  3D
// return str:  "OJAG=srid=tab=TYPE bbx  x y"  2D
void StringElementNode::makeDataString( const JagSchemaAttribute *attrs[], const char *buffers[],
										const Jstr &colobjstr, AbaxFixString &str )
{
	char *pbuf;
	int  ncols = _endcol - _begincol +1;
	int offset[ncols];
	int collen[ncols];
	int totlen = 0;
	for ( int i=0; i < ncols; ++i )
	{
		offset[i] = attrs[_tabnum][_begincol+i].offset;
		collen[i] = attrs[_tabnum][_begincol+i].length;
		totlen += collen[i];
	}
	int colobjsize = colobjstr.size();
	int extra = colobjsize + 24; // max 24 args on buffer 
	totlen += extra;
	const char *colobjptr = colobjstr.c_str();

	char *buf = jagmalloc( totlen );
	memset( buf, ' ', totlen );
	pbuf = buf;

	memcpy( pbuf, colobjptr, colobjsize );
	pbuf += colobjsize+1;

	for ( int i = 0; i < ncols; ++i ) {
		memcpy( pbuf, buffers[_tabnum]+offset[i], collen[i] );
		pbuf += collen[i]+1;
	}

	buf[totlen-1] = '\0';
	str = AbaxFixString( buf, totlen-1 );
	free( buf );
	//prt(("s0883 makeDataString str=[%s]\n", str.c_str() ));
}
#endif

// return str:  "OJAG=srid=tab=TYPE bbx  x y z"  3D
// return str:  "OJAG=srid=tab=TYPE bbx  x y"  2D
void StringElementNode::makeDataString( const JagSchemaAttribute *attrs[], const char *buffers[],
										const Jstr &colobjstr, AbaxFixString &str )
{
	int  ncols = _endcol - _begincol +1;
	Jstr bufstr = colobjstr;
	for ( int i = 0; i < ncols; ++i ) {
		bufstr += Jstr(" ") + Jstr(buffers[_tabnum]+attrs[_tabnum][_begincol+i].offset, attrs[_tabnum][_begincol+i].length ).trim0();
	}

	str = AbaxFixString( bufstr.c_str(), bufstr.size() );
}

// return str:  "OJAG=srid=tab=TYPE=subtype  x y z"  3D
// return str:  "OJAG=srid=tab=TYPE=subtype  x y"  2D
void StringElementNode::makeRangeDataString( const JagSchemaAttribute *attrs[], const char *buffers[],
										const Jstr &incolobjstr, AbaxFixString &str )
{
	//prt(("makeRangeDataString...\n" ));
	int  ncols = _endcol - _begincol +1;
	Jstr subtype;
	subtype = attrs[_tabnum][_begincol+0].type;
	Jstr bufstr = incolobjstr + "=" + subtype;
	for ( int i = 0; i < ncols; ++i ) {
		bufstr += Jstr(" ") + Jstr( buffers[_tabnum]+attrs[_tabnum][_begincol+i].offset, attrs[_tabnum][_begincol+i].length ).trim0();
	}
	str = AbaxFixString( bufstr.c_str(), bufstr.size() );
}

void StringElementNode::getPolyDataString( JagMergeReaderBase *ntr, const Jstr &polyType, 
											const JagHashStrInt *maps[],
											const JagSchemaAttribute *attrs[], const char *buffers[], 
											AbaxFixString &str )
{
	//prt(("s7026 getPolyDataString polyType=[%s]\n", polyType.c_str() ));
	if ( polyType == JAG_C_COL_TYPE_POLYGON 
		|| polyType == JAG_C_COL_TYPE_LINESTRING 
		|| polyType == JAG_C_COL_TYPE_MULTIPOINT
		|| polyType == JAG_C_COL_TYPE_MULTIPOLYGON
	    || polyType == JAG_C_COL_TYPE_MULTILINESTRING ) {
		getPolyData( polyType, ntr, maps, attrs, buffers, str, false );
	} else if ( polyType == JAG_C_COL_TYPE_POLYGON3D 
				|| polyType == JAG_C_COL_TYPE_LINESTRING3D
				|| polyType == JAG_C_COL_TYPE_MULTIPOINT3D
				|| polyType == JAG_C_COL_TYPE_MULTIPOLYGON3D
		        || polyType == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		getPolyData( polyType, ntr, maps, attrs, buffers, str, true );
	}
}

// return str "OJAG=srid=tab=TYPE  bbox[4]  x:y  x:y x:y ..."  linestring
// return str "OJAG=srid=tab=TYPE  bbox[6]  x:y:z  x:y:z ..."  linestring3d
// return str "OJAG=srid=tab=TYPE  bbox[4]  x:y  x:y x:y | x:y x:y ..."  polygon or multilinestring
// return str "OJAG=srid=tab=TYPE  bbox[6]  x:y:z  x:y:z x:y:z | x:y:z x:y:z | ..."  polygon3d or multilinestring3d
// return str "OJAG=srid=tab=TYPE  bbox[4]  x:y  x:y x:y | x:y x:y ... ! ...|...|...!...|...|..."  multipolygon
// return str "OJAG=srid=tab=TYPE  bbox[6]  x:y:z  x:y:z x:y:z | x:y:z x:y:z ... ! ...|...|...!...|...|..."  multipolygon3d
void StringElementNode::getPolyData( const Jstr &polyType, JagMergeReaderBase *ntr, 
									const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
									const char *buffers[], AbaxFixString &str, bool is3D )
{
	Jstr polyColumns;
	// prt(("s6732 polyType=[%s] _columns=[%s] is3D=%d\n", polyType.c_str(), _columns.c_str(), is3D ));
	prt(("s6732 polyType=[%s] is3D=%d\n", polyType.c_str(), is3D ));
	int acqpos;
	JagStrSplit dbc( _name, '.' );
	Jstr db = dbc[0];
	Jstr tab = dbc[1];
	//JagStrSplit sp( _columns, '|' );
	if ( _builder->_pparam->_allColumns.size() < 1 ) {
		if ( _builder->_pparam->_colHash ) {
			_builder->_pparam->_allColumns = _builder->_pparam->_colHash->getKeyStrings();
		}
	}
	prt(("s2283 allColumns=[%s]\n", _builder->_pparam->_allColumns.c_str() ));
	JagStrSplit sp( _builder->_pparam->_allColumns, '|' );

	Jstr colType;
	Jstr fullname;
	JagHashStrInt hash;
	for ( int i=0; i<sp.length(); ++i ) {
		fullname = db + "." + tab + "." + sp[i];
		if ( maps[_tabnum]->getValue( fullname, acqpos) ) {
			//prt(("s6732 acqpos=%d for sp[i]=[%s]\n", acqpos, sp[i].c_str() ));
			colType = attrs[_tabnum][acqpos].type;
			//prt(("s2208 colType=[%s]\n", colType.c_str() ));
			if ( JagParser::isPolyType( colType ) && ! hash.keyExist( sp[i] )  ) {
				if ( polyColumns.size() < 1 ) {
					polyColumns = sp[i];
				} else {
					polyColumns += Jstr("|") + sp[i];
				}
				hash.addKeyValue( sp[i], 1 );
				//prt(("s2098 hash.addKeyValue(%s)\n", sp[i].c_str() ));
			}
		} else {
			//prt(("s6732 no acqpos for sp[i]=[%s]\n", sp[i].c_str() ));
		}
	}
	//prt(("s7210 polyColumns=[%s]\n", polyColumns.c_str() ));

	bool isBoundBox3D = true;  // bbox is always 3D
	int ncols = attrs[_tabnum][0].record.columnVector->size();
	int offsetuuid=0;
	int collenuuid=0;
	int uuidcol=0;
	for ( int i=0; i < ncols; ++i ) {
		if ( 0==strcmp(attrs[_tabnum][i].colname.c_str(), "geo:id") ) {
			uuidcol = i;
			offsetuuid = attrs[_tabnum][i].offset;
			collenuuid = attrs[_tabnum][i].length;
			break;
		} 
	}

	// read buffers[_tabnum] and get the uuid of this row
	// The uuid may represent multiple linestrings or polygons, find the one that matches _name
	Jstr uuid(buffers[_tabnum]+offsetuuid, collenuuid );
	bool rc;
	//prt(("s2391 uuid=[%s] _rowUUID=[%s] this=%0x\n", uuid.c_str(), _builder->_pparam->_rowUUID.c_str(), this ));
	//prt(("s2391 _builder=%0x _builder->_pparam=%0x\n", _builder, _builder->_pparam ));
	//prt(("s2391 _builder->_pparam->_rowHash=%0x\n", _builder->_pparam->_rowHash ));
	if ( NULL == _builder->_pparam->_rowHash ) {
		_builder->_pparam->_rowHash = newObject<JagHashStrStr>();
		savePolyData( polyType, ntr, maps, attrs, buffers, uuid, db, tab, polyColumns, isBoundBox3D, is3D );
	} else if ( _builder->_pparam->_rowUUID.size() > 0 && uuid != _builder->_pparam->_rowUUID ) {
		//prt(("s1029 uuid != _rowUUID  delete _builder->_pparam->_rowHash \n" ));
		delete _builder->_pparam->_rowHash;
		//prt(("s6682 _builder->_pparam->_rowHash create new JagHashStrStr(); savePolyData... \n" ));
		_builder->_pparam->_rowHash = newObject<JagHashStrStr>();
		//prt(("s2392 _builder->_pparam->_rowHash=%0x\n", _builder->_pparam->_rowHash ));
		// build the hash map for _columns, check uniqueness
		savePolyData( polyType, ntr, maps, attrs, buffers, uuid, db, tab, polyColumns, isBoundBox3D, is3D );
	} 
	
	//prt(("s7409 _builder->_pparam=%0x _rowHash=%0x\n", _builder->_pparam, _builder->_pparam->_rowHash ));
	//prt(("s1028 _builder->_pparam->parent=%0x\n", _builder->_pparam->parent ));
	//prt(("s2342 _builder->_pparam->_rowHash=%0x\n", _builder->_pparam->_rowHash ));
	if ( _builder->_pparam->_rowHash ) {
		Jstr val = _builder->_pparam->_rowHash->getValue( _name, rc );
		if ( rc ) {
			str  = AbaxFixString(val.c_str(), val.size() );
			//prt(("s6720 got str=[%s] from _rowHash for _name=[%s]\n", str.c_str(), _name.c_str() ));
		} else {
			//prt(("6723 NOT got str=[%s] from _rowHash for _name=[%s]\n", str.c_str(), _name.c_str() ));
		}
		return;
	} else {
		//prt(("s6725 _rowHash is NULL\n" ));
		return; // error, no data
	}
}

/****************************************************************************************************************************
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[AczV45H5] geo:col=[13] geo:m=[1] geo:n=[1] geo:i=[1] po2:x=[0.0] po3:x=[] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[AczV45H5] geo:col=[13] geo:m=[1] geo:n=[1] geo:i=[2] po2:x=[20.0] po3:x=[] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[AczV45H5] geo:col=[13] geo:m=[1] geo:n=[1] geo:i=[3] po2:x=[8.0] po3:x=[] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[AczV45H5] geo:col=[13] geo:m=[1] geo:n=[1] geo:i=[4] po2:x=[0.0] po3:x=[] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[AczV45H5] geo:col=[13] geo:m=[1] geo:n=[2] geo:i=[1] po2:x=[1.0] po3:x=[] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[AczV45H5] geo:col=[13] geo:m=[1] geo:n=[2] geo:i=[2] po2:x=[2.0] po3:x=[] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[AczV45H5] geo:col=[13] geo:m=[1] geo:n=[2] geo:i=[3] po2:x=[1.0] po3:x=[] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[AczV45H5] geo:col=[16] geo:m=[1] geo:n=[1] geo:i=[1] po2:x=[] po3:x=[1.0] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[AczV45H5] geo:col=[16] geo:m=[1] geo:n=[1] geo:i=[2] po2:x=[] po3:x=[2.0] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[AczV45H5] geo:col=[16] geo:m=[1] geo:n=[1] geo:i=[3] po2:x=[] po3:x=[3.0] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[AczV45H5] geo:col=[16] geo:m=[1] geo:n=[1] geo:i=[4] po2:x=[] po3:x=[1.0] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[AczV45H5] geo:col=[16] geo:m=[1] geo:n=[2] geo:i=[1] po2:x=[] po3:x=[2.0] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[AczV45H5] geo:col=[16] geo:m=[1] geo:n=[2] geo:i=[2] po2:x=[] po3:x=[3.0] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[AczV45H5] geo:col=[16] geo:m=[1] geo:n=[2] geo:i=[3] po2:x=[] po3:x=[2.0] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[AczV45H5] geo:col=[21] geo:m=[1] geo:n=[1] geo:i=[1] po2:x=[] po3:x=[] ls:x=[300.0] ls:y=[400.0]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[AczV45H5] geo:col=[21] geo:m=[1] geo:n=[1] geo:i=[2] po2:x=[] po3:x=[] ls:x=[400.0] ls:y=[500.0]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[AczV45H5] geo:col=[21] geo:m=[1] geo:n=[1] geo:i=[3] po2:x=[] po3:x=[] ls:x=[500.0] ls:y=[600.0]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[UvO0wdS3] geo:col=[13] geo:m=[1] geo:n=[1] geo:i=[1] po2:x=[0.0] po3:x=[] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[UvO0wdS3] geo:col=[13] geo:m=[1] geo:n=[1] geo:i=[2] po2:x=[20.0] po3:x=[] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[UvO0wdS3] geo:col=[13] geo:m=[1] geo:n=[1] geo:i=[3] po2:x=[8.0] po3:x=[] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[UvO0wdS3] geo:col=[13] geo:m=[1] geo:n=[1] geo:i=[4] po2:x=[0.0] po3:x=[] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[UvO0wdS3] geo:col=[13] geo:m=[1] geo:n=[2] geo:i=[1] po2:x=[1.0] po3:x=[] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[UvO0wdS3] geo:col=[13] geo:m=[1] geo:n=[2] geo:i=[2] po2:x=[2.0] po3:x=[] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[UvO0wdS3] geo:col=[13] geo:m=[1] geo:n=[2] geo:i=[3] po2:x=[1.0] po3:x=[] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[UvO0wdS3] geo:col=[16] geo:m=[1] geo:n=[1] geo:i=[1] po2:x=[] po3:x=[1.0] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[UvO0wdS3] geo:col=[16] geo:m=[1] geo:n=[1] geo:i=[2] po2:x=[] po3:x=[2.0] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[UvO0wdS3] geo:col=[16] geo:m=[1] geo:n=[1] geo:i=[3] po2:x=[] po3:x=[3.0] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[UvO0wdS3] geo:col=[16] geo:m=[1] geo:n=[1] geo:i=[4] po2:x=[] po3:x=[1.0] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[UvO0wdS3] geo:col=[16] geo:m=[1] geo:n=[2] geo:i=[1] po2:x=[] po3:x=[2.0] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[UvO0wdS3] geo:col=[16] geo:m=[1] geo:n=[2] geo:i=[2] po2:x=[] po3:x=[3.0] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[UvO0wdS3] geo:col=[16] geo:m=[1] geo:n=[2] geo:i=[3] po2:x=[] po3:x=[2.0] ls:x=[] ls:y=[]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[UvO0wdS3] geo:col=[21] geo:m=[1] geo:n=[1] geo:i=[1] po2:x=[] po3:x=[] ls:x=[300.0] ls:y=[400.0]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[UvO0wdS3] geo:col=[21] geo:m=[1] geo:n=[1] geo:i=[2] po2:x=[] po3:x=[] ls:x=[400.0] ls:y=[500.0]
a=[2] geo:xmin=[0.0] geo:ymin=[0.0] geo:zmin=[1.0] id=[UvO0wdS3] geo:col=[21] geo:m=[1] geo:n=[1] geo:i=[3] po2:x=[] po3:x=[] ls:x=[500.0] ls:y=[600.0]
****************************************************************************************************************************/
// return str "OJAG=srid=tab=TYPE  bbox[4]  x:y  x:y x:y ..."  linestring
// return str "OJAG=srid=tab=TYPE  bbox[6]  x:y:z  x:y:z ..."  linestring3d
// return str "OJAG=srid=tab=TYPE  bbox[4]  x:y  x:y x:y | x:y x:y ..."  polygon or multilinestring
// return str "OJAG=srid=tab=TYPE  bbox[6]  x:y:z  x:y:z x:y:z | x:y:z x:y:z | ..."  polygon3d or multilinestring3d
// return str "OJAG=srid=tab=TYPE  bbox[4]  x:y  x:y x:y | x:y x:y ... ! ...|...|...!...|...|..."  multipolygon
// return str "OJAG=srid=tab=TYPE  bbox[6]  x:y:z  x:y:z x:y:z | x:y:z x:y:z ... ! ...|...|...!...|...|..."  multipolygon3d
void StringElementNode::savePolyData( const Jstr &polyType, JagMergeReaderBase *ntr, const JagHashStrInt *maps[], 
									const JagSchemaAttribute *attrs[], 
									const char *buffers[],  const Jstr &uuid,
									const Jstr &db, const Jstr &tab, const Jstr &polyColumns,
									bool isBoundBox3D, bool is3D )
{
	//prt(("s8410 savePolyData polyColumns=[%s] uuid=[%s] is3D=%d\n", polyColumns.c_str(), uuid.c_str(), is3D ));

	JagStrSplit psp(polyColumns, '|', true);
	int numCols = psp.length();
	int offsetx[numCols];
	int collenx[numCols];
	int offsety[numCols];
	int colleny[numCols];
	int offsetz[numCols];
	int collenz[numCols];
	Jstr colobjstr[numCols];
	Jstr fullname[numCols];
	Jstr str[numCols];
	Jstr nm;
	int acqpos;
	int xmincoloffset, xmincollen;
	bool rc;
	for ( int k=0; k < numCols; ++k ) {
		fullname[k] = db + "." + tab + "." + psp[k];
		if ( maps[_tabnum]->getValue( fullname[k], acqpos) ) {
			colobjstr[k] = Jstr( JAG_OJAG ) + "=" + intToStr( attrs[_tabnum][acqpos].srid ) 
			               + "=" + fullname[k] + "=" + attrs[_tabnum][acqpos].type + "=d";
		} else {
			//prt(("s6751 return\n"));
			return;
		}

		nm = fullname[k] + ":x";
		if ( maps[_tabnum]->getValue( nm, acqpos) ) {
			offsetx[k] =  attrs[_tabnum][acqpos].offset;
			collenx[k] =  attrs[_tabnum][acqpos].length;
			offsety[k] =  attrs[_tabnum][acqpos+1].offset;
			colleny[k] =  attrs[_tabnum][acqpos+1].length;
			if ( is3D ) {
				offsetz[k] =  attrs[_tabnum][acqpos+2].offset;
				collenz[k] =  attrs[_tabnum][acqpos+2].length;
			}
		} else {
			//prt(("s6752 return\n"));
	        return;
		}
	}

	int BBUUIDLEN = 7;
	JagBox bbox[BBUUIDLEN];  // bbox and uuid
	nm = db + "." + tab + ".geo:xmin";
	if ( maps[_tabnum]->getValue( nm, acqpos) ) {
		for ( int bi = 0; bi < BBUUIDLEN; ++bi ) {
			bbox[bi].col = acqpos;
			bbox[bi].offset = attrs[_tabnum][acqpos].offset;
			bbox[bi].len = attrs[_tabnum][acqpos].length;
			++acqpos;
		}
	} else {
		//prt(("s6753 return\n"));
		return;
	}

	// read buffers[_tabnum] and get the uuid of this row
	// The uuid may represent multiple linestrings or polygons, find the one that matches _name
	// int uuidcol = bbox[0].col + BBUUIDLEN-1;
	//prt(("s8801 buffers[_tabnum]: \n" ));
	//dumpmem( buffers[_tabnum], 700);

	Jstr v1, v2, v3, v4;
	int len = 0;
	bool hasValue = 0;
   	int bufsize;
	char *buf;

	for ( int k = 0; k < numCols; ++k ) {
    	bufsize =  colobjstr[k].size() + 1 + 12*JAG_GEOM_TOTLEN + 16;
    	buf = jagmalloc( bufsize );
    	len = 0;
    	memset(buf, ' ', bufsize );
		buf[bufsize-1] = 0;
    	memcpy( buf, colobjstr[k].c_str(), colobjstr[k].size() );
    	len +=  colobjstr[k].size() + 1;
		//prt(("s7001 len=%d\n", len ));
    
		if ( is3D ) {
    		for ( int j=0; j < BBUUIDLEN-1; ++j ) {
        		v1 = Jstr(buffers[_tabnum]+ bbox[j].offset, bbox[j].len );
        		v1.trimEndZeros();
        		memcpy( buf+len, v1.c_str(), v1.size() );
        		//len +=  v1.size();
        		len +=  v1.size();
    			if ( j < BBUUIDLEN-2 ) {
               		memcpy( buf+len, ":", 1 );
    			}
        		len += 1;
				//prt(("s7002 j=%d len=%d\n", j, len ));
    		}
		} else {
			char bbuf[200];
        	v1 = Jstr(buffers[_tabnum]+ bbox[0].offset, bbox[0].len ); v1.trimEndZeros();
        	v2 = Jstr(buffers[_tabnum]+ bbox[1].offset, bbox[1].len ); v2.trimEndZeros();
        	v3 = Jstr(buffers[_tabnum]+ bbox[3].offset, bbox[3].len ); v3.trimEndZeros();
        	v4 = Jstr(buffers[_tabnum]+ bbox[4].offset, bbox[4].len ); v4.trimEndZeros();
			sprintf( bbuf, "%s:%s:%s:%s", v1.c_str(), v2.c_str(), v3.c_str(), v4.c_str() );
        	memcpy( buf+len, bbuf, strlen(bbuf) );
			len += strlen(bbuf);
        	len += 1;
			//prt(("s7003 len=%d buf=[%s]\n", len, buf ));
		} 
    
    	Jstr xval(buffers[_tabnum]+offsetx[k], collenx[k] );
    	Jstr yval(buffers[_tabnum]+offsety[k], colleny[k] );
		//prt(("s70035 xval=[%s] yval=[%s] xval.size=%d yval.size=%d\n", xval.c_str(),  yval.c_str(), xval.size(),  yval.size() ));
    	xval.trimEndZeros();
    	yval.trimEndZeros();
		//prt(("s70036 xval=[%s] yval=[%s] xval.size=%d yval.size=%d\n", xval.c_str(),  yval.c_str(), xval.size(),  yval.size() ));
    	if ( xval.isNotNull() && yval.isNotNull() ) {
			/***
			prt(("s70035 xval=[%s] yval=[%s] xval.size=%d yval.size=%d buf=[%s] len=%d\n", 
					xval.c_str(),  yval.c_str(), xval.size(),  yval.size(), buf, len ));
			***/
           	memcpy( buf+len, xval.c_str(), xval.size() );
           	len += xval.size();
			//prt(("s7004 len=%d buf=[%s]\n", len, buf ));
           	memcpy( buf+len, ":", 1 );
           	len += 1;
           	memcpy( buf+len, yval.c_str(), yval.size() );
           	len +=  yval.size();
			hasValue = 1;
			//prt(("s7005 len=%d buf=[%s]\n", len, buf ));
    	}

		if ( is3D ) {
    		Jstr zval(buffers[_tabnum]+offsetz[k], collenz[k] );
    		zval.trimEndZeros();
			if ( zval.isNotNull() ) {
           		memcpy( buf+len, ":", 1 );
           		len += 1;
           		memcpy( buf+len, zval.c_str(), zval.size() );
           		len += zval.size();
				//prt(("s7006 len=%d buf=[%s]\n", len, buf ));
			}
		}

    	buf[len] = 0;
    	// str[k] = Jstr(buf, len );  // "OJAG=srid=tab=type bbox x:y:z
    	str[k] = Jstr(buf);  // "OJAG=srid=tab=type bbox x:y:z
    	//str[k] = Jstr(buf );  // "OJAG=srid=tab=type bbox x:y:z
		//prt(("s3408 len=%d strlen=%d\n", len, strlen( buf ) ));
		//prt(("s3921 buf=[%s] len=%d strlen=%d\n", buf, len, strlen(buf) ));
		free( buf );
    
    	if ( ! ntr ) {
			if ( hasValue ) {
    			_builder->_pparam->_rowHash->addKeyValue( fullname[k], str[k] );
			}
    		_builder->_pparam->_rowUUID = uuid;
			//prt(("s9393 return here\n" ));
    		return;
    	}
	} // end of numCols loop


	char xyzbuf[3*JAG_GEOM_TOTLEN+4];
	memset( xyzbuf, 0, 3*JAG_GEOM_TOTLEN+4);
	char zbuf[JAG_GEOM_TOTLEN+4];
	memset( zbuf, 0, JAG_GEOM_TOTLEN+4 );

	int geocoloffset, geocollen;
	int geomoffset, geomlen;
	int geonoffset, geonlen;
	int lastcol, lastm, lastn;

	nm = db + "." + tab + ".geo:col";
	if ( maps[_tabnum]->getValue( nm, acqpos) ) {
		geocoloffset =  attrs[_tabnum][acqpos].offset;
		geocollen =  attrs[_tabnum][acqpos].length;
	} else { return; }

	nm = db + "." + tab + ".geo:m";
	if ( maps[_tabnum]->getValue( nm, acqpos) ) {
		geomoffset =  attrs[_tabnum][acqpos].offset;
		geomlen =  attrs[_tabnum][acqpos].length;
	} else { return; }

	nm = db + "." + tab + ".geo:n";
	if ( maps[_tabnum]->getValue( nm, acqpos) ) {
		geonoffset =  attrs[_tabnum][acqpos].offset;
		geonlen =  attrs[_tabnum][acqpos].length;
	} else { return; }


	char *kvbuf = (char*)jagmalloc(ntr->KEYVALLEN+1);
	memset( kvbuf, 0, ntr->KEYVALLEN+1 );
	Jstr us;
	lastcol = lastm = lastn = -1;
	int newcol, newm, newn;
	char sep = 0;
	int numAdded[numCols];
	for ( int k = 0; k < numCols; ++k ) { numAdded[k] = 0; }

	int mgroup = 0;
	// 4:9:7|9:12:23|8:12:21:234
	// polygon1: ring1 has 4 points, ring2 has 9 points, ring3 has 7 points 
	// polygon2: ring1 has 9 points, ring2 has 12 points, ring3 has 23 points 
	// polygon3: ring1 has 8 points, ring2 has 12 points, ring3 has 21 points, ring4 has 234 points 
	while ( ntr->getNext(kvbuf) ) {
		//prt(("s2023 getNext kvbuf=:\n" ));
		//dumpmem( kvbuf, ntr->KEYVALLEN );
		//prt(("s8829 in savePolyData ntr->getNext(kvbuf) kvbuf=[%s]\n", kvbuf ));
		if ( 0 == strncmp(kvbuf+bbox[BBUUIDLEN-1].offset, uuid.c_str(), uuid.size() ) ) {
			newcol = jagatoi( kvbuf+geocoloffset, geocollen );
			newm = jagatoi( kvbuf+geomoffset, geomlen );
			newn = jagatoi( kvbuf+geonoffset, geonlen );
			//prt(("s8540 newcol=%d newm=%d newn=%d\n", newcol, newm, newn ));
			//prt(("s8540 newcol=%d lastm=%d lastn=%d\n", newcol, lastm, lastn ));
			if ( lastcol >=0 && newcol != lastcol ) {
				lastcol = newcol;
				lastm = -1;
				lastn = -1;
			} else if ( lastm >=0 && newm != lastm ) {
				lastcol = newcol;
				lastm = newm;
				lastn = -1;
				sep = '!';  // new polygon or new multistring
				++ mgroup;
			} else if ( lastn >=0 && newn != lastn ) {
				lastcol = newcol;
				lastm = newm;
				lastn = newn;
				sep = '|';  // new ring or new linestring
			} else {
				lastcol = newcol;
				lastm = newm;
				lastn = newn;
				sep = 0;
			}

			//prt(("s8540 newcol=%d newm=%d newn=%d lastcol=%d lastm=%d lastn=%d\n", newcol, newm, newn, lastcol, lastm, lastn ));
			//prt(("s8543 numCols=%d\n", numCols ));

   			for ( int k=0; k < numCols; ++k ) {
    			// each column  col1 col2
    			if ( *(kvbuf+offsetx[k]) != '\0' && *(kvbuf+offsety[k]) != '\0' ) {
    				v1 = Jstr( kvbuf+offsetx[k], collenx[k] );
    				v2 = Jstr( kvbuf+offsety[k], colleny[k] );
    				v1.trimEndZeros();
    				v2.trimEndZeros();
    				sprintf(xyzbuf, " %s:%s", v1.c_str(), v2.c_str() ); 
					if ( is3D ) {
    					v3 = Jstr( kvbuf+offsetz[k], collenz[k] );
    					v3.trimEndZeros();
						sprintf(zbuf, ":%s", v3.c_str() );
						strcat(xyzbuf, zbuf );
					}
					//prt(("s8203 xyzbuf=[%s] v1=[%s] v1size=%d v2=[%s] v2size=%d\n", xyzbuf, v1.c_str(), v1.size(), v2.c_str(), v2.size() ));

					if (  v1.isNotNull() && v2.isNotNull() ) {
						//prt(("s2209 sep=%c\n", sep ));
						if ( sep ) { 
							str[k] += ' ';
							str[k] += sep; 
						}
    					str[k] += Jstr(xyzbuf); // xyzbuf has ' ' in front
						//prt(("s2270 k=%d str[k]=[%s]\n", k, str[k].c_str() ));
						++ numAdded[k];
					} else {
						//prt(("s7304 v1 v2 either is NULL\n"));
					}
    			}
   			}
		} else {
			ntr->putBack( kvbuf );
			//prt(("s2040 putback kvbuf\n" ));
			break;
		}
	}  // end of getNext()

	for ( int k=0; k < numCols; ++k ) {
		if ( str[k].size() > 0 && numAdded[k] > 0 ) {
			rc = _builder->_pparam->_rowHash->addKeyValue( fullname[k], str[k] );
			//prt(("s6713 add k=%d name=[%s] str=[%s] rc=%d _rHash=%0x\n", k, fullname[k].c_str(), str[k].c_str(), rc, _builder->_pparam->_rowHash ));
		} else {
			//prt(("s4691 str[%d].size() < 1 no _rowHash->addKeyValue\n", k ));
		}
	}
	_builder->_pparam->_rowUUID = uuid;
	//prt(("s4809 _rowUUID=[%s]\n", _builder->_pparam->_rowUUID.c_str() ));
	/**
	a=[2] geo:xmin=[2.9] geo:ymin=[2] geo:xmax=[77] geo:ymax=[88] 
	geo:id=[492a...E7] geo:col=[13] geo:i=[3] ls1:x=[] ls1:y=[] b=[210] ls2:x=[8.9] ls2:y=[9]
	**/
	// need find geo:xmin column, then sequentially get all :* columns
	// str result: "HDR xmin:ymin:xmax:ymax x1:y1 x2:y2  x3:y3 ..."
	free( kvbuf );
}




////////////////////////////////////////////// BinaryOpNode ////////////////////////////////////
// ctor
BinaryOpNode::BinaryOpNode( BinaryExpressionBuilder* builder, short op, ExprElementNode *l, ExprElementNode *r,
    				const JagParseAttribute &jpa, int arg1, int arg2, Jstr carg1 )
{
	_binaryOp = op; _left = l; _right = r; _jpa = jpa; _arg1 = arg1; _arg2 = arg2; _carg1 = carg1;
	_numCnts = _initK = _stddevSum = _stddevSumSqr = _nodenum = 0;
	_builder = builder;
	_isElement = false;
	_reg = NULL;
}

void BinaryOpNode::clear()
{
	if ( _left ) { _left->clear(); if ( _left ) delete _left; _left = NULL; }
	if ( _right ) { _right->clear(); if ( _right ) delete _right; _right = NULL; }
	if ( _reg ) { delete _reg; _reg = NULL; }
}

// return -1: error; 
// 0: OK with all range; 
// 1 OK with limited range
int BinaryOpNode::setWhereRange( const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
						const int keylen[], const int numKeys[], int numTabs, bool &hasValue, 
						JagMinMax *minmax, AbaxFixString &str, int &typeMode, int &tabnum ) 
{
	//prt(("s0448 BinaryOpNode::setWhereRange numTabs=%d ...\n", numTabs ));

	str = "";
	JagMinMax leftbuf[numTabs];
	JagMinMax rightbuf[numTabs];
	for ( int i = 0; i < numTabs; ++i ) {
		leftbuf[i].setbuflen( keylen[i] );
		//prt(("s9983 leftbuf[%d] setbuflen=%d\n", i, keylen[i] ));

		rightbuf[i].setbuflen( keylen[i] );
		//prt(("s9984 rightbuf[%d] setbuflen=%d\n", i, keylen[i] ));
	}

	bool lhasVal = 0, rhasVal = 0;
	AbaxFixString lstr, rstr;
	int leftVal = 1, rightVal = 1, ltmode = 0, rtmode = 0, ltabnum = -1, rtabnum = -1, result = 0;
	if ( _left ) leftVal = _left->setWhereRange( maps, attrs, keylen, numKeys, numTabs, 
									lhasVal, leftbuf, lstr, ltmode, ltabnum );
	if ( _right ) rightVal = _right->setWhereRange( maps, attrs, keylen, numKeys, numTabs, 
									rhasVal, rightbuf, rstr, rtmode, rtabnum );
	hasValue = lhasVal || rhasVal;

	//prt(("s6638 in BinaryOpNode::setWhereRange leftVal=%d rightVal=%d\n", leftVal, rightVal ));

    if ( leftVal < 0 || rightVal < 0 ) {
		//prt(("s2730 leftVal=%d rightVal=%d -1\n", leftVal, rightVal ));
		result = -1;
	} else if ( isAggregateOp(_binaryOp) ) {
		//prt(("s2730 isAggregateOp(_binaryOp=%d) -2\n", _binaryOp ));
		result = -2; // no aggregation allowed in where tree
	} else {
		result = _doWhereCalc( maps, attrs, keylen, numKeys, numTabs, ltmode, rtmode, ltabnum, rtabnum,
							  minmax, leftbuf, rightbuf, str, lstr, rstr );
		//prt(("s7293 in BinaryOpNode::setWhereRange _doWhereCalc() result=%d\n", result ));
		if ( result > 0 ) {
			if ( leftVal == 0 && rightVal == 0 ) result = 0;
			else if ( leftVal == 1 && rightVal == 1 ) result = 1;
			else if ( leftVal != rightVal && JAG_LOGIC_OR == _binaryOp ) result = 0;
			else if ( leftVal != rightVal && JAG_LOGIC_AND == _binaryOp ) result = 1;
			// OK
			else result = 0;  // no range, want all data
			// result 1: has range; 0: has no range
			//prt(("s8804 adjusted result=%d\n", result ));
		} else {
			//prt(("s2283 result=%d\n", result ));
		}
	}
	
	if ( ltmode > rtmode ) typeMode = ltmode;
	else typeMode = rtmode;
	tabnum = -1;
	//prt(("s7370 BinaryOpNode::setWhereRange result=%d\n", result ));
	
    return result;
}


void BinaryOpNode::print( int mode ) {
	if ( 0 == mode ) {
		if ( _left && _right) {
			prt(("L(")); _left->print( mode ); prt((")"));
			prt((" [op=%s] ", binaryOpStr( _binaryOp ).c_str()));
			prt(("R(")); _right->print( mode );  prt((")"));
		} else if ( _left && !_right ) {
			prt((" [op=%s] ", binaryOpStr( _binaryOp ).c_str()));
			prt(("L(")); _left->print( mode ); prt((")"));
		} else if ( !_left && _right ) {
			prt((" [op=%s] ", binaryOpStr( _binaryOp ).c_str()));
			prt(("R(")); _right->print( mode ); prt((")"));
		}
	} else {
		if ( _left && _right) {
			prt(("L(")); _left->print( mode ); prt((")"));
			prt((" [op=%d] ", _binaryOp));
			prt(("R(")); _right->print( mode );  prt((")"));
		} else if ( _left && !_right ) {
			prt((" [op=%d] ", _binaryOp));
			prt(("L(")); _left->print( mode ); prt((")"));
		} else if ( !_left && _right ) {
			prt((" [op=%d] ", _binaryOp));
			prt(("R(")); _right->print( mode ); prt((")"));
		} else {
			prt(("\nBadBinCode=%d\n",  _binaryOp ));
		}
	}
}

Jstr BinaryOpNode::binaryOpStr( short binaryOp )
{
	Jstr str;
	if ( binaryOp == JAG_FUNC_EQUAL ) str = "=";
	else if ( binaryOp == JAG_FUNC_NOTEQUAL ) str = "!=";
	else if ( binaryOp == JAG_FUNC_LESSTHAN ) str = "<";
	else if ( binaryOp == JAG_FUNC_LESSEQUAL ) str = "<=";
	else if ( binaryOp == JAG_FUNC_GREATERTHAN ) str = ">";
	else if ( binaryOp == JAG_FUNC_GREATEREQUAL ) str = ">=";
	else if ( binaryOp == JAG_FUNC_LIKE ) str = "like";
	else if ( binaryOp == JAG_FUNC_MATCH ) str = "match";
	else if ( binaryOp == JAG_LOGIC_AND ) str = "and";
	else if ( binaryOp == JAG_LOGIC_OR ) str = "or";
	else if ( binaryOp == JAG_NUM_ADD ) str = "+";
	else if ( binaryOp == JAG_NUM_SUB ) str = "-";
	else if ( binaryOp == JAG_NUM_MULT ) str = "*";
	else if ( binaryOp == JAG_NUM_DIV ) str = "/";
	else if ( binaryOp == JAG_NUM_REM ) str = "%";
	else if ( binaryOp == JAG_NUM_POW ) str = "^";
	else if ( binaryOp == JAG_FUNC_POW ) str = "pow";
	else if ( binaryOp == JAG_FUNC_MOD ) str = "mod";
	else if ( binaryOp == JAG_FUNC_LENGTH ) str = "length";
	else if ( binaryOp == JAG_FUNC_LINELENGTH ) str = "linelength";
	else if ( binaryOp == JAG_FUNC_ABS ) str = "abs";
	else if ( binaryOp == JAG_FUNC_ACOS ) str = "acos";
	else if ( binaryOp == JAG_FUNC_ASIN ) str = "asin";
	else if ( binaryOp == JAG_FUNC_ATAN ) str = "atan";
	else if ( binaryOp == JAG_FUNC_COS ) str = "cos";
	else if ( binaryOp == JAG_FUNC_STRDIFF ) str = "strdiff";
	else if ( binaryOp == JAG_FUNC_SIN ) str = "sin";
	else if ( binaryOp == JAG_FUNC_TAN ) str = "tan";
	else if ( binaryOp == JAG_FUNC_COT ) str = "cot";
	else if ( binaryOp == JAG_FUNC_LOG2 ) str = "log2";
	else if ( binaryOp == JAG_FUNC_LOG10 ) str = "log10";
	else if ( binaryOp == JAG_FUNC_LOG ) str = "ln";
	else if ( binaryOp == JAG_FUNC_DEGREES ) str = "degrees";
	else if ( binaryOp == JAG_FUNC_RADIANS ) str = "radians";
	else if ( binaryOp == JAG_FUNC_SQRT ) str = "sqrt";
	else if ( binaryOp == JAG_FUNC_CEIL ) str = "ceil";
	else if ( binaryOp == JAG_FUNC_FLOOR ) str = "floor";
	else if ( binaryOp == JAG_FUNC_UPPER ) str = "upper";
	else if ( binaryOp == JAG_FUNC_LOWER ) str = "lower";
	else if ( binaryOp == JAG_FUNC_LTRIM ) str = "ltrim";
	else if ( binaryOp == JAG_FUNC_RTRIM ) str = "rtrim";
	else if ( binaryOp == JAG_FUNC_TRIM ) str = "trim";
	else if ( binaryOp == JAG_FUNC_SUBSTR ) str = "substr";
	else if ( binaryOp == JAG_FUNC_SECOND ) str = "second";
	else if ( binaryOp == JAG_FUNC_MINUTE ) str = "minute";
	else if ( binaryOp == JAG_FUNC_HOUR ) str = "hour";
	else if ( binaryOp == JAG_FUNC_DATE ) str = "date";
	else if ( binaryOp == JAG_FUNC_MONTH ) str = "month";
	else if ( binaryOp == JAG_FUNC_YEAR ) str = "year";
	else if ( binaryOp == JAG_FUNC_DATEDIFF ) str = "datediff";
	else if ( binaryOp == JAG_FUNC_DAYOFMONTH ) str = "dayofmonth";
	else if ( binaryOp == JAG_FUNC_DAYOFWEEK ) str = "dayofweek";
	else if ( binaryOp == JAG_FUNC_DAYOFYEAR ) str = "dayofyear";
	else if ( binaryOp == JAG_FUNC_CURDATE ) str = "curdate";
	else if ( binaryOp == JAG_FUNC_CURTIME ) str = "curtime";
	else if ( binaryOp == JAG_FUNC_NOW ) str = "now";
	else if ( binaryOp == JAG_FUNC_TIME ) str = "time";
	else if ( binaryOp == JAG_FUNC_PI ) str = "pi";
	else if ( binaryOp == JAG_FUNC_DISTANCE ) str = "distance";
	else if ( binaryOp == JAG_FUNC_CONTAIN ) str = "contain";
	else if ( binaryOp == JAG_FUNC_SAME ) str = "equal";
	else if ( binaryOp == JAG_FUNC_WITHIN ) str = "within";
	else if ( binaryOp == JAG_FUNC_CLOSESTPOINT ) str = "closestpoint";
	else if ( binaryOp == JAG_FUNC_ANGLE ) str = "angle";
	else if ( binaryOp == JAG_FUNC_AREA ) str = "area";
	else if ( binaryOp == JAG_FUNC_PERIMETER ) str = "perimeter";
	else if ( binaryOp == JAG_FUNC_VOLUME ) str = "volume";
	else if ( binaryOp == JAG_FUNC_DIMENSION ) str = "dimension";
	else if ( binaryOp == JAG_FUNC_GEOTYPE ) str = "geotype";
	else if ( binaryOp == JAG_FUNC_ISCLOSED ) str = "isclosed";
	else if ( binaryOp == JAG_FUNC_ISSIMPLE ) str = "issimple";
	else if ( binaryOp == JAG_FUNC_ISVALID ) str = "isvalid";
	else if ( binaryOp == JAG_FUNC_ISRING ) str = "isring";
	else if ( binaryOp == JAG_FUNC_ISPOLYGONCCW ) str = "ispolygonccw";
	else if ( binaryOp == JAG_FUNC_ISPOLYGONCW ) str = "ispolygoncw";
	else if ( binaryOp == JAG_FUNC_ISCONVEX ) str = "isconvex";
	else if ( binaryOp == JAG_FUNC_POINTN ) str = "pointn";
	else if ( binaryOp == JAG_FUNC_BBOX ) str = "bbox";
	else if ( binaryOp == JAG_FUNC_STARTPOINT ) str = "startpoint";
	else if ( binaryOp == JAG_FUNC_CONVEXHULL ) str = "convexhull";
	else if ( binaryOp == JAG_FUNC_TOPOLYGON ) str = "topolygon";
	else if ( binaryOp == JAG_FUNC_UNION ) str = "union";
	else if ( binaryOp == JAG_FUNC_LOCATEPOINT ) str = "locatepoint";
	else if ( binaryOp == JAG_FUNC_INTERSECTION ) str = "intersection";
	else if ( binaryOp == JAG_FUNC_DIFFERENCE ) str = "difference";
	else if ( binaryOp == JAG_FUNC_SYMDIFFERENCE ) str = "symdifference";
	else if ( binaryOp == JAG_FUNC_COLLECT ) str = "collect";
	else if ( binaryOp == JAG_FUNC_OUTERRING ) str = "outerring";
	else if ( binaryOp == JAG_FUNC_OUTERRINGS ) str = "outerrings";
	else if ( binaryOp == JAG_FUNC_INNERRINGS ) str = "innerrings";
	else if ( binaryOp == JAG_FUNC_RINGN ) str = "ringn";
	else if ( binaryOp == JAG_FUNC_POLYGONN ) str = "polygonn";
	else if ( binaryOp == JAG_FUNC_ASTEXT ) str = "astext";
	else if ( binaryOp == JAG_FUNC_UNIQUE ) str = "unique";
	else if ( binaryOp == JAG_FUNC_BUFFER ) str = "buffer";
	else if ( binaryOp == JAG_FUNC_ENDPOINT ) str = "endpoint";
	else if ( binaryOp == JAG_FUNC_NUMPOINTS ) str = "numpoints";
	else if ( binaryOp == JAG_FUNC_NUMSEGMENTS ) str = "numsegments";
	else if ( binaryOp == JAG_FUNC_NUMRINGS ) str = "numrings";
	else if ( binaryOp == JAG_FUNC_NUMPOLYGONS ) str = "numpolygons";
	else if ( binaryOp == JAG_FUNC_NUMINNERRINGS ) str = "numinnerrings";
	else if ( binaryOp == JAG_FUNC_SRID ) str = "srid";
	else if ( binaryOp == JAG_FUNC_SUMMARY ) str = "summary";
	else if ( binaryOp == JAG_FUNC_XMIN ) str = "xmin";
	else if ( binaryOp == JAG_FUNC_YMIN ) str = "ymin";
	else if ( binaryOp == JAG_FUNC_ZMIN ) str = "zmin";
	else if ( binaryOp == JAG_FUNC_XMAX ) str = "xmax";
	else if ( binaryOp == JAG_FUNC_YMAX ) str = "ymax";
	else if ( binaryOp == JAG_FUNC_ZMAX ) str = "zmax";
	else if ( binaryOp == JAG_FUNC_COVEREDBY ) str = "coveredby";
	else if ( binaryOp == JAG_FUNC_COVER ) str = "cover";
	else if ( binaryOp == JAG_FUNC_INTERSECT ) str = "intersect";
	else if ( binaryOp == JAG_FUNC_DISJOINT ) str = "disjoint";
	else if ( binaryOp == JAG_FUNC_NEARBY ) str = "nearby";
	else if ( binaryOp == JAG_FUNC_ALL ) str = "all";
	else if ( binaryOp == JAG_FUNC_CENTROID ) str = "centroid";
	else if ( binaryOp == JAG_FUNC_REVERSE ) str = "reverse";
	else if ( binaryOp == JAG_FUNC_SCALE ) str = "scale";
	else if ( binaryOp == JAG_FUNC_SCALEAT ) str = "scaleat";
	else if ( binaryOp == JAG_FUNC_SCALESIZE ) str = "scalesize";
	else if ( binaryOp == JAG_FUNC_TRANSLATE ) str = "translate";
	else if ( binaryOp == JAG_FUNC_TRANSSCALE ) str = "transscale";
	else if ( binaryOp == JAG_FUNC_ROTATE ) str = "rotate";
	else if ( binaryOp == JAG_FUNC_ROTATEAT ) str = "rotateat";
	else if ( binaryOp == JAG_FUNC_ROTATESELF ) str = "rotateself";
	else if ( binaryOp == JAG_FUNC_AFFINE ) str = "affine";
	return str;
}


// return 0: error, invalid aggregation; return 1: OK
int BinaryOpNode::setFuncAttribute( const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
	int &constMode, int &typeMode, bool &isAggregate, Jstr &type, int &collen, int &siglen )
{
	prt(("s5829 setFuncAttr left=%0x right=%0x _binaryOp=%d ...\n", _left, _right, _binaryOp ));

	if ( !_left && !_right ) {
		// funcs without childs, e.g. NOW, CURTIME, CURDATE
		typeMode = 0;
		constMode = 0;
		isAggregate = 0;
		type = JAG_C_COL_TYPE_STR;
		collen = getFuncLength( _binaryOp );
		siglen = 0;
		return 1;
	}

	bool laggr = 0, raggr = 0;
	Jstr ltype, rtype;
	int leftVal = 1, rightVal = 1, ltmode = 0, rtmode = 0, lcmode = 0, rcmode = 0, 
		lcollen = 0, rcollen = 0, lsiglen = 0, rsiglen = 0;
    if ( _left ) leftVal = _left->setFuncAttribute( maps, attrs, lcmode, ltmode, laggr, ltype, lcollen, lsiglen );
	if ( _right ) rightVal = _right->setFuncAttribute( maps, attrs, rcmode, rtmode, raggr, rtype, rcollen, rsiglen );
	prt(("s1029 _left=%0x leftVal=%d rightVal=%d\n", _left, leftVal, rightVal ));

	if ( !_left || !leftVal || !rightVal ) {
		// invalid tree, error
		prt(("s1026 Invalid tree error\n" ));
		return 0;
	}

	if ( !checkAggregateValid( lcmode, rcmode, laggr, raggr ) ) {
		// aggregate combine with non aggregate funcs, error
		return 0;
	}
		
	//prt(("s2238 _right=%0x\n", _right ));
	if ( _binaryOp == JAG_FUNC_CONVEXHULL || _binaryOp == JAG_FUNC_BUFFER || _binaryOp == JAG_FUNC_UNIQUE
	     || _binaryOp == JAG_FUNC_RINGN || _binaryOp == JAG_FUNC_INNERRINGN ||  _binaryOp == JAG_FUNC_POLYGONN
		 || _binaryOp == JAG_FUNC_UNION || _binaryOp == JAG_FUNC_COLLECT || _binaryOp == JAG_FUNC_INTERSECTION
		 || _binaryOp == JAG_FUNC_LINESUBSTRING || _binaryOp == JAG_FUNC_ADDPOINT || _binaryOp == JAG_FUNC_SETPOINT
		 || _binaryOp == JAG_FUNC_REMOVEPOINT || _binaryOp == JAG_FUNC_REVERSE ||  _binaryOp == JAG_FUNC_TRANSLATE
		 || _binaryOp == JAG_FUNC_SCALE || _binaryOp == JAG_FUNC_SCALEAT || _binaryOp == JAG_FUNC_SCALESIZE
		 || _binaryOp == JAG_FUNC_TRANSSCALE || _binaryOp == JAG_FUNC_ROTATE || _binaryOp == JAG_FUNC_ROTATEAT
		 || _binaryOp == JAG_FUNC_ROTATESELF || _binaryOp == JAG_FUNC_AFFINE
		 || _binaryOp == JAG_FUNC_ASTEXT ||  _binaryOp == JAG_FUNC_DIFFERENCE || _binaryOp == JAG_FUNC_SYMDIFFERENCE
		 || _binaryOp == JAG_FUNC_OUTERRING || _binaryOp == JAG_FUNC_OUTERRINGS || _binaryOp == JAG_FUNC_INNERRINGS ) {
		ltmode = 0;
		type = JAG_C_COL_TYPE_STR;
		collen = JAG_MAX_POINTS_SENT*JAG_POINT_LEN + 2;
		siglen = 0;
		// todo can we get number of points of lstr?
	} else if ( _binaryOp == JAG_FUNC_TOPOLYGON ) {
		ltmode = 0;
		type = JAG_C_COL_TYPE_STR;
		collen = JAG_MAX_POINTS_SENT*JAG_POINT_LEN/3 + 2;
		siglen = 0;
	} else if ( _binaryOp == JAG_FUNC_POINTN ||  _binaryOp == JAG_FUNC_STARTPOINT || _binaryOp == JAG_FUNC_ENDPOINT
				|| _binaryOp == JAG_FUNC_INTERPOLATE
                || _binaryOp == JAG_FUNC_CENTROID || _binaryOp == JAG_FUNC_CLOSESTPOINT ) {
		ltmode = 0;
		type = JAG_C_COL_TYPE_STR;
		collen = 3*JAG_POINT_LEN + 2;
		siglen = 0;
	} else if ( _binaryOp == JAG_FUNC_ANGLE || _binaryOp == JAG_FUNC_LINELENGTH ||  _binaryOp == JAG_FUNC_LOCATEPOINT ) {
		ltmode = 0;
		type = JAG_C_COL_TYPE_STR;
		collen = JAG_POINT_LEN + 2;
		siglen = 0;
	} else if ( _binaryOp == JAG_FUNC_BBOX ) {
		ltmode = 0;
		type = JAG_C_COL_TYPE_STR;
		collen = 6*JAG_POINT_LEN + 5;
		siglen = 0;
	} else if ( _binaryOp == JAG_FUNC_GEOTYPE ) {
		ltmode = 0;
		type = JAG_C_COL_TYPE_STR;
		collen = 32;
		siglen = 0;
	} else if ( _binaryOp == JAG_FUNC_ISCLOSED ||  _binaryOp == JAG_FUNC_ISSIMPLE || _binaryOp == JAG_FUNC_ISCONVEX 
			    || _binaryOp == JAG_FUNC_ISRING || _binaryOp == JAG_FUNC_ISPOLYGONCCW || _binaryOp == JAG_FUNC_ISPOLYGONCW
			    ||  _binaryOp == JAG_FUNC_ISVALID ) {
		//ltmode = 0;
		ltmode = 1;
		type = JAG_C_COL_TYPE_STR;
		collen = 2;
		siglen = 0;
	} else if ( _binaryOp == JAG_FUNC_SRID || _binaryOp == JAG_FUNC_NUMPOINTS 
				 || _binaryOp == JAG_FUNC_DIMENSION || _binaryOp == JAG_FUNC_NUMSEGMENTS
				 ||  _binaryOp == JAG_FUNC_NUMPOLYGONS
	             || _binaryOp == JAG_FUNC_NUMRINGS || _binaryOp == JAG_FUNC_NUMINNERRINGS ) {
		// important for integer
		ltmode = 1;
		type = JAG_C_COL_TYPE_DINT;
		collen = JAG_DINT_FIELD_LEN;
		siglen = 0;
	} else if ( _binaryOp == JAG_FUNC_SUMMARY ) {
		ltmode = 0;
		type = JAG_C_COL_TYPE_STR;
		collen = 128; 
		siglen = 0;
	} else if ( _binaryOp == JAG_FUNC_XMIN || _binaryOp == JAG_FUNC_YMIN || _binaryOp == JAG_FUNC_ZMIN
	            || _binaryOp == JAG_FUNC_XMAX || _binaryOp == JAG_FUNC_YMAX || _binaryOp == JAG_FUNC_ZMAX ) {
		ltmode = 2;
		type = JAG_C_COL_TYPE_DOUBLE;
		collen = JAG_DOUBLE_FIELD_LEN; 
		siglen = JAG_DOUBLE_SIG_LEN;
	} else if ( !_right ) {
		// one child
		//prt(("s6512 one left child ltmode=%d\n", ltmode ));
		if ( 0 == ltmode && 
			(( isStringOp(_binaryOp) || isSpecialOp(_binaryOp) || isTimedateOp(_binaryOp) ) 
			   && _binaryOp != JAG_FUNC_LENGTH ) ) {
			if ( isTimedateOp(_binaryOp) ) {
				ltmode = 0;
				type = JAG_C_COL_TYPE_STR;
				collen = JAG_DATETIMENANO_FIELD_LEN-6;
				siglen = 0;
			} else {
				type = ltype;
				collen = lcollen;
				siglen = lsiglen;
				//prt(("s8120 type=[%s] lcollen=%d \n", type.c_str(), lcollen ));
			}
		} else {
			//prt(("s2035 JAG_C_COL_TYPE_FLOAT JAG_MAX_INT_LEN JAG_MAX_SIG_LEN\n" ));
			ltmode = 2;
			type = JAG_C_COL_TYPE_FLOAT;
			collen = JAG_MAX_INT_LEN;
			siglen = JAG_MAX_SIG_LEN;
		}
	} else {
		// two children
		prt(("s7102 two children JAG_NUM_ADD ltmode=%d rtmode=%d\n", ltmode, rtmode ));
		if ( 0 == ltmode && 0 == rtmode && _binaryOp == JAG_NUM_ADD ) {
			type = ltype;
			collen = lcollen + rcollen;
			siglen = 0;
		} else {
			ltmode = 2;
			type = JAG_C_COL_TYPE_FLOAT;
			collen = JAG_MAX_INT_LEN;
			siglen = JAG_MAX_SIG_LEN;
		}
	}
	
	if ( isAggregateOp(_binaryOp) || lcmode == 2 || rcmode == 2 ) {
		isAggregate = 1;
	}

	if ( ltmode > rtmode ) typeMode = ltmode;
	else typeMode = rtmode;

	if ( isAggregate ) {
		constMode = 2;
	} else {
		if ( lcmode > rcmode ) constMode = lcmode;
		else constMode = rcmode;
	}
	
	//prt(("s5882 collen=%d type=%s ltmode=%d\n", collen, type.c_str(), ltmode ));
    return 1;
}



int BinaryOpNode::getFuncAggregate( JagVector<Jstr> &selectParts, JagVector<int> &selectPartsOpcode, 
	JagVector<int> &selColSetAggParts, JagHashMap<AbaxInt, AbaxInt> &selColToselParts, int &nodenum, int treenum )
{	
	//prt(("s0392 tmp9999 BinaryOpNode::getFuncAggregate() ...\n" ));
	// check if operator is aggregation, and format possible funcs if yes
	if ( isAggregateOp( _binaryOp ) ) {
		// get aggregate parts, only left child exists
		Jstr parts, parts2;
		_left->getAggregateParts( parts, nodenum );
		_nodenum = nodenum++;		
		if ( _binaryOp == JAG_FUNC_MIN ) {
			parts = Jstr("min(") + parts + ")";
		} else if ( _binaryOp == JAG_FUNC_MAX ) {
			parts = Jstr("max(") + parts + ")";
		} else if ( _binaryOp == JAG_FUNC_AVG ) {
			parts = Jstr("avg(") + parts + ")";
		} else if ( _binaryOp == JAG_FUNC_SUM ) {
			parts = Jstr("sum(") + parts + ")";
		} else if ( _binaryOp == JAG_FUNC_COUNT ) {
			parts = Jstr("count(") + parts + ")";
			//prt(("s8131 JAG_FUNC_COUNT parts=[%s]\n", parts.c_str() ));
		} else if ( _binaryOp == JAG_FUNC_STDDEV ) {
			parts2 = Jstr("avg(") + parts + ")";
			parts = Jstr("stddev(") + parts + ")";
		} else if ( _binaryOp == JAG_FUNC_FIRST ) {
			parts = Jstr("first(") + parts + ")";
		} else if ( _binaryOp == JAG_FUNC_LAST ) {
			parts = Jstr("last(") + parts + ")";
		}
		
		if ( parts2.length() > 0 ) {
			selectParts.append( parts2 );
			selectPartsOpcode.append( JAG_FUNC_AVG );
			selColSetAggParts.append( _nodenum );
			selColToselParts.setValue( selectParts.length()-1, treenum, true );
		}
		selectParts.append( parts );
		selectPartsOpcode.append( _binaryOp );
		selColSetAggParts.append( _nodenum );
		selColToselParts.setValue( selectParts.length()-1, treenum, true );
	} else {
		if ( _left ) _left->getFuncAggregate( selectParts, selectPartsOpcode, selColSetAggParts, 
			selColToselParts, nodenum, treenum );
		if ( _right ) _right->getFuncAggregate( selectParts, selectPartsOpcode, selColSetAggParts, 
			selColToselParts, nodenum, treenum );
		_nodenum = nodenum++;
		
	}
	return 1;
}


int BinaryOpNode::getAggregateParts( Jstr &parts, int &nodenum )
{
	Jstr lparts, rparts;
	if ( _left ) _left->getAggregateParts( lparts, nodenum );
	if ( _right ) _right->getAggregateParts( rparts, nodenum );
	formatAggregateParts( parts, lparts, rparts );
	_nodenum = nodenum++;
	return 1;
}


int BinaryOpNode::setAggregateValue( int nodenum, const char *buf, int length )
{
	if ( _left ) _left->setAggregateValue( nodenum, buf, length );
	if ( _right ) _right->setAggregateValue( nodenum, buf, length );
	if ( nodenum == _nodenum ) {
		// if avg, sum, stddev, convert to abaxdouble
		// otherwise, use as string
		if ( _binaryOp == JAG_FUNC_AVG || _binaryOp == JAG_FUNC_SUM || _binaryOp == JAG_FUNC_STDDEV ) {
			_opString = longDoubleToStr(raystrtold(buf, length));
		} else {
			_opString = AbaxFixString( buf, length );
		}
	}
	return 1;
}


// if useZero, when result = 0 after _doCalculation, use "0" for this data, otherwise, ignore this line
// return -1: error and false; 
// 0: OK and false ( e.g. 1 == 0 ) ;
// 1 and 2 OK and true ( e.g. 1 == 1 )  2: if no data avaialble
int BinaryOpNode::checkFuncValid( JagMergeReaderBase *ntr, const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[],
								         const char *buffers[], AbaxFixString &str, int &typeMode, Jstr &type, 
										 int &length, bool &first, bool useZero, bool setGlobal )
{
	prt(("s2301 BinaryOpNode::checkFuncValid _left=%0x _right=%0x _binaryOp=%d ...\n", _left, _right, _binaryOp ));

	AbaxFixString lstr, rstr;
	Jstr ltype, rtype;
	int leftVal = 1, rightVal = 1, ltmode = 0, rtmode = 0, llength = 0, rlength = 0, result = 0;

	if ( first && setGlobal ) {
		_opString = ""; _numCnts = _initK = _stddevSum = _stddevSumSqr = 0;
	}

    if ( _left ) {
		// prt(("s3938 tmp9999 _left->checkFuncValid() ...\n" ));
		leftVal = _left->checkFuncValid( ntr, maps, attrs, buffers, lstr, ltmode, ltype, llength, first, useZero, setGlobal );
		//prt(("s3938 tmp9999 _left->checkFuncValid() done lstr=[%s] leftVal=%d\n", lstr.c_str(), leftVal ));
	}

    if ( _right ) {
		// prt(("s3939 tmp9999 _right->checkFuncValid() ...\n" ));
		rightVal = _right->checkFuncValid( ntr, maps, attrs, buffers, rstr, rtmode, rtype, rlength, first, useZero, setGlobal );
		//prt(("s3939 tmp9999 _right->checkFuncValid() done rstr=[%s] rightVal=%d\n", rstr.c_str(), rightVal ));
	}

	if ( leftVal < 0 || rightVal < 0 ) { 
		result = -1; 
		prt(("s0392  leftVal < 0 || rightVal < 0  result = -1\n" ));
	} else {
		if ( JAG_LOGIC_OR == _binaryOp ) {
			result = leftVal || rightVal;
			// prt(("s2032 result = leftVal || rightVal result=%d\n", result ));
		} else if ( JAG_LOGIC_AND == _binaryOp ) {
			result = leftVal && rightVal;
			// prt(("s2334 result = leftVal && rightVal  result=%d\n", result ));
		} else if ( 2 == leftVal || 2 == rightVal ) {
			if ( 0 == leftVal || 0 == rightVal ) result = 0;
			else result = 2;
			// prt(("s2335 weird  result=%d\n", result ));
		} else {
			// prt(("s2039 in BinaryOpNode::checkFuncValid() \n" ));
			prt(("s2039 _doCalculation _binaryOp=%d lstr=[%s] ltmd=%d ltp=%s\n", _binaryOp, lstr.c_str(), ltmode, ltype.c_str() ));
			// prt(("s2039 _doCalculation rstr=[%s] ...\n", rstr.c_str() ));
			result = _doCalculation( lstr, rstr, ltmode, rtmode, ltype, rtype, llength, rlength, first );
			if ( result < 0 && setGlobal ) {
				_opString = ""; _numCnts = _initK = _stddevSum = _stddevSumSqr = 0;
			}	
			if ( useZero ) {
				if ( 0 == result ) result = 1;
			}
		}

		if ( ltmode > rtmode ) typeMode = ltmode;
		else typeMode = rtmode;

		if ( ltype.size() > 0 ) {
			type = ltype;
			length = llength;
			str = lstr;
			if ( setGlobal ) _opString = str;
		} else {
			type = rtype;
			length = rlength;
			str = lstr;   // not a bug, must be lstr
			// str = rstr;  // cannot be rstr
			if ( setGlobal ) _opString = str;
		}
	}
	
	//prt(("s0939 end  BinaryOpNode::checkFuncValid tmp9999 result=%d\n", result ));
	//prt(("s3008 str=[%s] setGlobal=%d typeMode=%d\n", str.c_str(), setGlobal, typeMode ));
	return result;
}

// return -1: error; 
// 0: OK and false ( e.g. 1 == 0 ) ; 
// 1 OK and true ( e.g. 1 == 1 )
int BinaryOpNode::checkFuncValidConstantOnly( AbaxFixString &str, int &typeMode, Jstr &type, int &length )
{
	prt(("s1128 BinaryOpNode::checkFuncValidConstantOnly _left=%0x _right=%0x\n", _left, _right ));

	bool first = 0;
	AbaxFixString lstr, rstr;
	Jstr ltype, rtype;
	int leftVal = 1, rightVal = 1, ltmode = 0, rtmode = 0, llength = 0, rlength = 0, result = 0;

	if ( _left ) {
		leftVal = _left->checkFuncValidConstantOnly( lstr, ltmode, ltype, llength );
		prt(("s1177 leftVal=%d lstr=[%s]\n", leftVal, lstr.c_str() ));
	}
	if ( _right ) {
		rightVal = _right->checkFuncValidConstantOnly( rstr, rtmode, rtype, rlength );
		prt(("s1178 rightVal=%d rstr=[%s]\n", rightVal, rstr.c_str() ));
	}

	if ( leftVal < 0 || rightVal < 0 ) result = -1;
	else {	
		if ( !isAggregateOp( _binaryOp ) ) {
			if ( lstr.length() < 1 && rstr.length() < 1 ) return result;
			if ( JAG_LOGIC_OR == _binaryOp ) {
				result = leftVal || rightVal;
			} else if ( JAG_LOGIC_AND == _binaryOp ) {
				result = leftVal && rightVal;
			} else {
				prt(("s2810 in BinaryOpNode::checkFuncValidConstantOnly before _doCalculation \n" ));
				prt(("s2810 lstr=[%s]\n", lstr.c_str() ));
				prt(("s2810 rstr=[%s]\n", rstr.c_str() ));
				result = _doCalculation( lstr, rstr, ltmode, rtmode, ltype, rtype, llength, rlength, first );
				prt(("s1143 _doCalculation result=[%d] lstr=[%s]\n", result, lstr.c_str() ));
				if ( result < 0 ) {
					_opString = ""; _numCnts = _initK = _stddevSum = _stddevSumSqr = 0;
				} else result = 1;
			}

			if ( ltmode > rtmode ) typeMode = ltmode;
			else typeMode = rtmode;
			prt(("s1491 ltype=%s rtype=%s typeMode=%d\n", ltype.c_str(), rtype.c_str(), typeMode ));

			if ( ltype.size() > 0 ) {
				type = ltype;
				length = llength;
				str = _opString = lstr;
				prt(("s1029 ltype >=  str=%s\n", str.c_str() ));
			} else {
				type = rtype;
				length = rlength;
				str = _opString = lstr;  
				//str = _opString = rstr;
				prt(("s1031 ltype <= 0 str=%s\n", str.c_str() ));
			}

		} else {
			// ignore, since aggregate value has already been set
			str = _opString;
			// change ltmode for aggregate funcs
			typeMode = 2;
		}
	}
	
	return result;
}

void BinaryOpNode::findOrBuffer( JagMinMax *minmax, JagMinMax *leftbuf, JagMinMax *rightbuf, 
	const int keylen[], const int numTabs )
{
	for ( int i = 0; i < numTabs; ++i ) {
		//minbuf
		if ( memcmp(leftbuf[i].minbuf, rightbuf[i].minbuf, keylen[i]) <= 0 ) {
			memcpy(minmax[i].minbuf, leftbuf[i].minbuf, keylen[i]);
		} else {
			memcpy(minmax[i].minbuf, rightbuf[i].minbuf, keylen[i]);				
		} 
		//maxbuf
		if ( memcmp(leftbuf[i].maxbuf, rightbuf[i].maxbuf, keylen[i]) >= 0 ) {
			memcpy(minmax[i].maxbuf, leftbuf[i].maxbuf, keylen[i]);			
		} else {
			memcpy(minmax[i].maxbuf, rightbuf[i].maxbuf, keylen[i]);				
		}
	}
}

void BinaryOpNode::findAndBuffer( JagMinMax *minmax, JagMinMax *leftbuf, JagMinMax *rightbuf, 
	const JagSchemaAttribute *attrs[], const int numTabs, const int numKeys[] )
{
	abaxint offset;
	for ( int i = 0; i < numTabs; ++i ) {
		offset = 0;
		for ( int j = 0; j < numKeys[i]; ++j ) {
			//minbuf
			if ( memcmp(leftbuf[i].minbuf+offset, rightbuf[i].minbuf+offset, attrs[i][j].length) >= 0 ) {
				memcpy(minmax[i].minbuf+offset, leftbuf[i].minbuf+offset, attrs[i][j].length);
			} else {
				memcpy(minmax[i].minbuf+offset, rightbuf[i].minbuf+offset, attrs[i][j].length);				
			} 
			//maxbuf
			if ( memcmp(leftbuf[i].maxbuf+offset, rightbuf[i].maxbuf+offset, attrs[i][j].length) <= 0 ) {
				memcpy(minmax[i].maxbuf+offset, leftbuf[i].maxbuf+offset, attrs[i][j].length);				
			} else {
				memcpy(minmax[i].maxbuf+offset, rightbuf[i].maxbuf+offset, attrs[i][j].length);				
			} 
			offset += attrs[i][j].length;
		}
	}	
}

// minOrMax 0: both min max
// minOrMax 1: min only
// minOrMax 2: max only
bool BinaryOpNode::formatColumnData( JagMinMax *minmax, JagMinMax *iminmax, 
										    const AbaxFixString &value, int tabnum, int minOrMax )
{	
	// if is value, not set column data
	if ( minmax[tabnum].buflen <= iminmax[tabnum].offset ) { 
		/***
		prt(("s2930 not minmax[%d].buflen=%d  <= iminmax[%d].offset=%d minmax[tabnum].offset=%d minmax[tabnum].length=%d\n", 
				tabnum,  minmax[tabnum].buflen, tabnum, iminmax[tabnum].offset, minmax[tabnum].offset, 
				minmax[tabnum].length  ));
		prt(("s0290 iminmax[tabnum].offset=%d iminmax[tabnum].length=%d\n", iminmax[tabnum].offset, iminmax[tabnum].length ));
		prt(("s0283 minmax[tabnum].type=[%s] iminmax[tabnum].type=[%s]\n", minmax[tabnum].type.c_str(), iminmax[tabnum].type.c_str() ));
		prt(("s0283 minmax[tabnum].colname=[%s] iminmax[tabnum].colname=[%s]\n", minmax[tabnum].colname.c_str(), iminmax[tabnum].colname.c_str() ));
		// return false; 
		***/

		//prt(("s0291 formatColumnData return true %d <= %d\n", minmax[tabnum].buflen, iminmax[tabnum].offset ));
		return true; 
	}

	//prt(("s2027 formatColumnData minOrMax=%d tabnum=%d value=[%s]\n", minOrMax, tabnum, value.c_str() ));
	char *buf = minmax[tabnum].minbuf;
	if ( 2 == minOrMax ) {
		buf = minmax[tabnum].maxbuf;
	}
	//prt(("s2028 formatColumnData buf=[%s]\n", buf ));

	Jstr errmsg;
	//char c = *(buf+iminmax[tabnum].offset+iminmax[tabnum].length);

	formatOneCol( _jpa.timediff, _jpa.servtimediff, buf, value.c_str(), errmsg, "GARBAGE",
				  iminmax[tabnum].offset, iminmax[tabnum].length, iminmax[tabnum].sig, 
				  iminmax[tabnum].type );

	// natural format -> db format, one column only
	dbNaturalFormatExchange( buf, 0, NULL, iminmax[tabnum].offset, iminmax[tabnum].length, 
							 iminmax[tabnum].type ); 


	//c = *(buf+iminmax[tabnum].offset+iminmax[tabnum].length);
	if ( 0 == minOrMax ) {
		if ( iminmax[tabnum].offset+iminmax[tabnum].length > minmax[tabnum].buflen ) {
			prt(("s8283 error\n" ));
		} else {
			memcpy( minmax[tabnum].maxbuf+iminmax[tabnum].offset, buf+iminmax[tabnum].offset, iminmax[tabnum].length);
		}
		/***
		prt(("s7903 memcpy iminmax: offset=%d len=%d\n", iminmax[tabnum].offset, iminmax[tabnum].length ));
		prt(("s8162 buf+iminmax[tabnum].offset=[%s]\n", buf+iminmax[tabnum].offset ));
		int len1 = iminmax[tabnum].offset+iminmax[tabnum].length;
		prt(("s2287 len1=%d keylen=%d iminmax[tabnum].sig=%d\n", len1, keylen, iminmax[tabnum].sig ));
		prt(("s2287 minmax[tabnum].buflen=%d iminmax[tabnum].buflen=%d \n", minmax[tabnum].buflen, iminmax[tabnum].buflen ));
		***/
	}

	return true;
}

bool BinaryOpNode::checkAggregateValid( int lcmode, int rcmode, bool laggr, bool raggr )
{

	if ( _right ) { // mathOp, mod, pow, datediff -- two children op
		if ( (2 == lcmode && 1 == rcmode) || (1 == lcmode && 2 == rcmode) ) return 0; // one aggregate and one column, error
	} else { // most functions -- one child op
		if ( 2 == lcmode && isAggregateOp(_binaryOp) ) return 0; // aggregate of aggregate func, error
	}
	return 1;
}

// method to format aggregate parts
int BinaryOpNode::formatAggregateParts( Jstr &parts, Jstr &lparts, Jstr &rparts )
{
	// no aggregate funcs process in this method
	// = ==
	if ( _binaryOp == JAG_FUNC_EQUAL ) {
		parts = Jstr("(") + lparts + ")=(" + rparts + ")";
	}
	// != <> ><
	else if ( _binaryOp == JAG_FUNC_NOTEQUAL ) {
		parts = Jstr("(") + lparts + ")!=(" + rparts + ")";
	}
	// < || <=
	else if ( _binaryOp == JAG_FUNC_LESSTHAN ) {
		parts = Jstr("(") + lparts + ")<(" + rparts + ")";
	} else if ( _binaryOp == JAG_FUNC_LESSEQUAL ) {
		parts = Jstr("(") + lparts + ")<=(" + rparts + ")";	
	}
	// > || >=
	else if ( _binaryOp == JAG_FUNC_GREATERTHAN ) {
		parts = Jstr("(") + lparts + ")>(" + rparts + ")";	
	} else if ( _binaryOp == JAG_FUNC_GREATEREQUAL ) {
		parts = Jstr("(") + lparts + ")>=(" + rparts + ")";		
	}
	// like
	else if ( _binaryOp == JAG_FUNC_LIKE ) {
		parts = Jstr("(") + lparts + ") like (" + rparts + ")";
	}
	// match
	else if ( _binaryOp == JAG_FUNC_MATCH ) {
		parts = Jstr("(") + lparts + ") match (" + rparts + ")";
	}
	// +
	else if ( _binaryOp == JAG_NUM_ADD ) {
		parts = Jstr("(") + lparts + ")+(" + rparts + ")";
	}	
	// - 
	else if ( _binaryOp == JAG_NUM_SUB ) {
		parts = Jstr("(") + lparts + ")-(" + rparts + ")";
	}	
	// *
	else if ( _binaryOp == JAG_NUM_MULT ) {
		parts = Jstr("(") + lparts + ")*(" + rparts + ")";	
	}	
	// /
	else if ( _binaryOp == JAG_NUM_DIV ) {
		parts = Jstr("(") + lparts + ")/(" + rparts + ")";
	}
	// %
	else if ( _binaryOp == JAG_NUM_REM ) {
		parts = Jstr("(") + lparts + ")%(" + rparts + ")";
	}	
	// ^
	else if ( _binaryOp == JAG_NUM_POW || _binaryOp == JAG_FUNC_POW ) {
		parts = Jstr("(") + lparts + ")^(" + rparts + ")";
	}
	// MOD
	else if ( _binaryOp == JAG_FUNC_MOD ) {
		parts = Jstr("mod(") + lparts + "," + rparts + ")";
	}
	// LENGTH
	else if ( _binaryOp == JAG_FUNC_LENGTH ) {
		parts = Jstr("length(") + lparts + ")";
	}
	// ABS
	else if ( _binaryOp == JAG_FUNC_ABS ) {
		parts = Jstr("abs(") + lparts + ")";
	}
	// ACOS
	else if ( _binaryOp == JAG_FUNC_ACOS ) {
		parts = Jstr("acos(") + lparts + ")";
	}	
	// ASIN
	else if ( _binaryOp == JAG_FUNC_ASIN ) {
		parts = Jstr("asin(") + lparts + ")";
	}	
	// ATAN
	else if ( _binaryOp == JAG_FUNC_ATAN ) {
		parts = Jstr("atan(") + lparts + ")";
	}
	// COS
	else if ( _binaryOp == JAG_FUNC_COS ) {
		parts = Jstr("cos(") + lparts + ")";
	}		
	// DIFF
	else if ( _binaryOp == JAG_FUNC_STRDIFF ) {
		parts = Jstr("diff(") + lparts + "," + rparts + ")";
	}		
	// SIN
	else if ( _binaryOp == JAG_FUNC_SIN ) {
		parts = Jstr("sin(") + lparts + ")";		
	}
	// TAN
	else if ( _binaryOp == JAG_FUNC_TAN ) {
		parts = Jstr("tan(") + lparts + ")";
	}	
	// COT
	else if ( _binaryOp == JAG_FUNC_COT ) {
		parts = Jstr("cot(") + lparts + ")";
	}	
	// ALL
	else if ( _binaryOp == JAG_FUNC_ALL ) {
		prt(("s0291 JAG_FUNC_ALL \n" ));
		parts = Jstr("all(") + lparts + ")";
	}	
	// LOG2
	else if ( _binaryOp == JAG_FUNC_LOG2 ) {
		parts = Jstr("log2(") + lparts + ")";
	}	
	// LOG10
	else if ( _binaryOp == JAG_FUNC_LOG10 ) {
		parts = Jstr("log10(") + lparts + ")";
	}	
	// LOG
	else if ( _binaryOp == JAG_FUNC_LOG ) {
		parts = Jstr("ln(") + lparts + ")";
	}
	// DEGREE
	else if ( _binaryOp == JAG_FUNC_DEGREES ) {
		parts = Jstr("degrees(") + lparts + ")";
	}
	// RADIAN
	else if ( _binaryOp == JAG_FUNC_RADIANS ) {
		parts = Jstr("radians(") + lparts + ")";
	}
	// SQRT
	else if ( _binaryOp == JAG_FUNC_SQRT ) {
		parts = Jstr("sqrt(") + lparts + ")";
	}
	// CEIL
	else if ( _binaryOp == JAG_FUNC_CEIL ) {
		parts = Jstr("ceil(") + lparts + ")";
	}	
	// FLOOR
	else if ( _binaryOp == JAG_FUNC_FLOOR ) {
		parts = Jstr("floor(") + lparts + ")";
	}
	// UPPER
	else if ( _binaryOp == JAG_FUNC_UPPER ) {
		parts = Jstr("upper(") + lparts + ")";
	}	
	// LOWER
	else if ( _binaryOp == JAG_FUNC_LOWER ) {
		parts = Jstr("lower(") + lparts + ")";
	}	
	// LTRIM
	else if ( _binaryOp == JAG_FUNC_LTRIM ) {
		parts = Jstr("ltrim(") + lparts + ")";
	}	
	// RTRIM
	else if ( _binaryOp == JAG_FUNC_RTRIM ) {
		parts = Jstr("rtrim(") + lparts + ")";
	}	
	// TRIM
	else if ( _binaryOp == JAG_FUNC_TRIM ) {
		parts = Jstr("trim(") + lparts + ")";
	}
	// date, time, datetime etc
	else if ( _binaryOp == JAG_FUNC_SECOND ) {
		parts = Jstr("second(") + lparts + ")";
	}
	else if ( _binaryOp == JAG_FUNC_MINUTE ) {
		parts = Jstr("minute(") + lparts + ")";
	}
	else if ( _binaryOp == JAG_FUNC_HOUR ) {
		parts = Jstr("hour(") + lparts + ")";
	}
	else if ( _binaryOp == JAG_FUNC_DATE ) {
		parts = Jstr("date(") + lparts + ")";
	}
	else if ( _binaryOp == JAG_FUNC_MONTH ) {
		parts = Jstr("month(") + lparts + ")";
	}
	else if ( _binaryOp == JAG_FUNC_YEAR ) {
		parts = Jstr("year(") + lparts + ")";
	}
	else if ( _binaryOp == JAG_FUNC_DATEDIFF ) {
		parts = Jstr("datediff(");
		if ( *(_carg1.c_str()) == 's' ) parts += "second,";
		else if ( *(_carg1.c_str()) == 'm' ) parts += "minute,";
		else if ( *(_carg1.c_str()) == 'h' ) parts += "hour,";
		else if ( *(_carg1.c_str()) == 'D' ) parts += "day,";
		else if ( *(_carg1.c_str()) == 'M' ) parts += "month,";
		else if ( *(_carg1.c_str()) == 'Y' ) parts += "year,";
		parts += lparts + "," + rparts + ")";
	}
	else if ( _binaryOp == JAG_FUNC_DAYOFMONTH ) {
		parts = Jstr("dayofmonth(") + lparts + ")";
		
	}
	else if ( _binaryOp == JAG_FUNC_DAYOFWEEK ) {
		parts = Jstr("dayofweek(") + lparts + ")";
	}
	else if ( _binaryOp == JAG_FUNC_DAYOFYEAR ) {
		parts = Jstr("dayofyear(") + lparts + ")";
	}
	// SUBSTR
	else if ( _binaryOp == JAG_FUNC_SUBSTR ) {
		parts = Jstr("substr(") + lparts + "," + intToStr(_arg1) + "," + intToStr(_arg2);
		if ( _carg1.length() > 0 ) {
			// has encoding
			parts += Jstr(",") + _carg1;
		}
		parts += ")";
		//prt(("s2883 JAG_FUNC_SUBSTR parts=[%s]\n", parts.c_str() ));
	}
	// CURDATE, CURTIME, NOW with no child funcs
	else if ( _binaryOp == JAG_FUNC_CURDATE ) {
		parts = "curdate()";
	} else if ( _binaryOp == JAG_FUNC_CURTIME ) {
		parts = "curtime()";
	} else if ( _binaryOp == JAG_FUNC_NOW ) {
		parts = "now()";
	} else if ( _binaryOp == JAG_FUNC_TIME ) {
		parts = "time()";
	} else if ( _binaryOp == JAG_FUNC_DISTANCE ) {
		// prt(("s8384 tmp9999 parts=[%s]\n", parts.c_str() ));
		//parts = Jstr("distance(") + lparts + "," + rparts + ")";
		parts = Jstr("distance(") + lparts + "," + rparts + ", " + _carg1 + ")";
	} else if ( _binaryOp == JAG_FUNC_WITHIN ) {
		parts = Jstr("within(") + lparts + "," + rparts + ")";
	} else if ( _binaryOp == JAG_FUNC_COVEREDBY ) {
		parts = Jstr("coveredby(") + lparts + "," + rparts + ")";
	} else if ( _binaryOp == JAG_FUNC_COVER ) {
		parts = Jstr("cover(") + lparts + "," + rparts + ")";
	} else if ( _binaryOp == JAG_FUNC_CONTAIN ) {
		parts = Jstr("contain(") + lparts + "," + rparts + ")";
	} else if ( _binaryOp == JAG_FUNC_INTERSECT ) {
		parts = Jstr("intersect(") + lparts + "," + rparts + ")";
	} else if ( _binaryOp == JAG_FUNC_DISJOINT ) {
		parts = Jstr("disjoint(") + lparts + "," + rparts + ")";
	} else if ( _binaryOp == JAG_FUNC_NEARBY ) {
		parts = Jstr("nearby(") + lparts + "," + rparts + ", " + _carg1 + ")";
	} else if ( _binaryOp == JAG_FUNC_AREA ) {
		parts = Jstr("area(") + lparts + ")";
	} else if ( _binaryOp == JAG_FUNC_DIMENSION ) {
		parts = Jstr("dimension(") + lparts + ")";
	} else if ( _binaryOp == JAG_FUNC_GEOTYPE ) {
		parts = Jstr("geotype(") + lparts + ")";
	/***
	} else if ( _binaryOp == JAG_FUNC_CLOSESTPOINT ) {
		parts = Jstr("closestpoint(") + lparts + "," + rparts + ")";
	***/
	}
	// ... more calcuations ...
	return 1;
}  // end of formatAggregateParts()

int BinaryOpNode::_doWhereCalc( const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
						const int keylen[], const int numKeys[], int numTabs, int ltmode, int rtmode, 
						int ltabnum, int rtabnum,
						JagMinMax *minmax, JagMinMax *lminmax, JagMinMax *rminmax, 
						AbaxFixString &str, AbaxFixString &lstr, AbaxFixString &rstr )
{
	char c;
	int coltype, cmode;

	/***
	prt(("s3302  enter BinaryOpNode::doWhereCalc _binaryOpStr=[%s] _binaryOp=%d ltabnum=%d rtabnum=%d ...\n", 
			binaryOpStr( _binaryOp ).c_str(), _binaryOp, ltabnum, rtabnum ));
	prt(("s3303 BinaryOpNode::doWhereCalc  lstr=[%s] lstr.size=%d\n", lstr.c_str(), lstr.length() ));
	prt(("s3303 BinaryOpNode::doWhereCalc  rstr=[%s] rstr.size=%d\n", rstr.c_str(),  rstr.length() ));
	***/

	if ( lstr.length() < 1 && rstr.length() < 1 ) {
		//prt(("s0313 doWhereCalc return 0\n" ));
		return 0; 
	}

	//prt(("s9293 ...\n" ));
	// both left and right are columns, return 0
	if ( lstr.length() < 1 && rstr.length() > 0 && ltabnum >= 0 ) coltype = 1; 
	// one column( left ) and one constant or equation( right )
	else if ( lstr.length() > 0 && rstr.length() < 1 && rtabnum >= 0 ) coltype = 2; 
	// one column( right ) and one constant or equation( left )
	else coltype = 0; // two constants or equations

	if ( ltmode > rtmode ) cmode = ltmode;
	else cmode = rtmode;

	//prt(("s2042 coltype=%d\n", coltype ));

	// = ==
	if ( _binaryOp == JAG_FUNC_EQUAL ) {
		if ( 1 == coltype ) {
			if ( formatColumnData( minmax, lminmax, rstr, ltabnum, 0 ) ) {
				str = AbaxFixString(minmax[ltabnum].minbuf, keylen[ltabnum]);
			} else {
				str="1";
			}
			//prt(("s5002 JAG_FUNC_EQUAL 1 == coltype str=[%s] return 1\n", str.c_str() ));
			return 1;
		} else if ( 2 == coltype ) {
			if ( formatColumnData( minmax, rminmax, lstr, rtabnum, 0 ) ) {
				str = AbaxFixString(minmax[rtabnum].minbuf, keylen[rtabnum]);
			} else {
				str="1";
			}
			// prt(("s5003 JAG_FUNC_EQUAL 2 == coltype str=[%s] return 1\n", str.c_str() ));
			return 1;
		} else {
			// prt(("s0293 JAG_FUNC_EQUAL coltype=%d cmode=%d \n", coltype, cmode ));
			// prt(("s0293 JAG_FUNC_EQUAL lstr=[%s]\n", lstr.c_str() ));
			// prt(("s0293 JAG_FUNC_EQUAL rstr=[%s]\n", lstr.c_str() ));

			if ( 2 == cmode && jagstrtold(lstr.c_str() ) != jagstrtold(rstr.c_str() ) ) {
				// double/float compare not equal
				return 0; 
			} else if ( 1 == cmode && jagatoll(lstr.c_str()) != jagatoll(rstr.c_str()) ) {
				// int compare not equal
				return 0; 
			} else if ( strcmp(lstr.c_str(), rstr.c_str()) != 0 ) {
				// string compare not equal
				return 0; 
			}

			str = "1";
			return 1;			
		}
	}
	// != <> ><
	else if ( _binaryOp == JAG_FUNC_NOTEQUAL ) {
		if ( 0 == coltype ) {
			if ( 2 == cmode && jagstrtold(lstr.c_str(), NULL) == jagstrtold(rstr.c_str(), NULL) ) return 0; 
			// double/float compare not equal
			else if ( 1 == cmode && jagatoll(lstr.c_str()) == jagatoll(rstr.c_str()) ) return 0; 
			// int compare not equal
			else if ( strcmp(lstr.c_str(), rstr.c_str()) == 0 ) return 0; 
			// string compare not equal
			str = "1";
			return 1;
		}
	}
	// < || <=
	else if ( _binaryOp == JAG_FUNC_LESSTHAN || _binaryOp == JAG_FUNC_LESSEQUAL ) {
		#if 0
		prt(("s0304 JAG_FUNC_LESSTHAN or JAG_FUNC_LESSEQUAL lstr=[%s] rstr=[%s] ltabnum=%d coltype=%d ...\n", 
				lstr.c_str(), rstr.c_str(), ltabnum, coltype ));
		#endif
		//prt(("s3047 JAG_FUNC_LESSTHAN/JAG_FUNC_LESSEQUAL coltype=%d\n", coltype ));

		if ( 1 == coltype ) {
			//prt(("s9309 ltabnum=%d keylen[ltabnum]=%d\n", ltabnum, keylen[ltabnum] ));
			if ( formatColumnData( minmax, lminmax, rstr, ltabnum, 2 ) ) {
				str = AbaxFixString(minmax[ltabnum].maxbuf, keylen[ltabnum] );
				//prt(("s3865 1 == coltype str=[%s] return 1\n", str.c_str() ));
			} else {
				str = "1";
			}
			return 1;
		} else if ( 2 == coltype ) {
			if ( formatColumnData( minmax, rminmax, lstr, rtabnum, 1 ) ) {
				str = AbaxFixString(minmax[rtabnum].minbuf, keylen[rtabnum]);
			} else {
				str = "1";
			}
			// prt(("s3034 2 == coltype str=[%s] return 1\n", str.c_str() ));
			return 1;
		} else {
			if ( _binaryOp == JAG_FUNC_LESSTHAN ) {
				// double/float compare not equal
				if ( 2 == cmode && jagstrtold(lstr.c_str() ) >= jagstrtold(rstr.c_str() ) ) {
					return 0; 
				} else if ( 1 == cmode && jagatoll(lstr.c_str()) >= jagatoll(rstr.c_str()) ) {
					return 0; 
				} else if ( strcmp(lstr.c_str(), rstr.c_str()) >= 0 ) {
					// int compare not equal
					return 0; 
				}
				// string compare not equal
			} else if ( _binaryOp == JAG_FUNC_LESSEQUAL ) {
				// double/float compare not equal
				if ( 2 == cmode && jagstrtold(lstr.c_str() ) > jagstrtold(rstr.c_str() ) ) {
					return 0; 
				} else if ( 1 == cmode && jagatoll(lstr.c_str()) > jagatoll(rstr.c_str()) ) {
					return 0; 
				} else if ( strcmp(lstr.c_str(), rstr.c_str()) > 0 ) {
					return 0; 
				}
				// int compare not equal
				// string compare not equal
			} else {
				//prt(("s8839 _binaryOp=[%s]\n", binaryOpStr( _binaryOp ).c_str() ));
			}
			str = "1";
			//prt(("s0393 tmp9999 str=1 dummy return 1 _binaryOp=[%s]\n", binaryOpStr( _binaryOp ).c_str() ));
			return 1;
		}
	}
	// > || >=
	else if ( _binaryOp == JAG_FUNC_GREATERTHAN || _binaryOp == JAG_FUNC_GREATEREQUAL ) {
		if ( 1 == coltype ) {
			//prt(("s0028 formatColumnData ...\n" ));
			formatColumnData( minmax, lminmax, rstr, ltabnum, 1  );
			str = AbaxFixString(minmax[ltabnum].minbuf, keylen[ltabnum]);
			//prt(("s0028 formatColumnData str=[%s] ...\n", str.c_str()  ));
			return 1;
		} else if ( 2 == coltype ) {
			formatColumnData( minmax, rminmax, lstr, rtabnum, 2  );
			str = AbaxFixString(minmax[rtabnum].maxbuf, keylen[rtabnum]);
			return 1;
		} else {
			if ( _binaryOp == JAG_FUNC_GREATERTHAN ) {
				if ( 2 == cmode && jagstrtold(lstr.c_str(), NULL) <= jagstrtold(rstr.c_str(), NULL) ) return 0; 
				else if ( 1 == cmode && jagatoll(lstr.c_str()) <= jagatoll(rstr.c_str()) ) return 0; 
				else if ( strcmp(lstr.c_str(), rstr.c_str()) <= 0 ) return 0; 
			} else if ( _binaryOp == JAG_FUNC_GREATEREQUAL ) {
				if ( 2 == cmode && jagstrtold(lstr.c_str(), NULL) < jagstrtold(rstr.c_str(), NULL) ) return 0; 
				else if ( 1 == cmode && jagatoll(lstr.c_str()) < jagatoll(rstr.c_str()) ) return 0; 
				else if ( strcmp(lstr.c_str(), rstr.c_str()) < 0 ) return 0; 
			}
			str = "1";
			return 1;
		}
	}
	// like
	else if ( _binaryOp == JAG_FUNC_LIKE ) {
		if ( 1 == coltype ) {
			char *pfirst = (char*)rstr.c_str();
			char *plast = strrchr( (char*)rstr.c_str(), '%');
			if ( *pfirst != '%' && plast ) {
				char onebyte = 255;
				char minbuf[plast-pfirst+1];
				char maxbuf[plast-pfirst+1];
				minbuf[plast-pfirst] = '\0';
				maxbuf[plast-pfirst] = '\0';
				//prt(("s7650 pfirst=[%s] plast=[%s]\n", pfirst, plast ));
				memcpy(minbuf, pfirst, plast-pfirst);
				memcpy(maxbuf, pfirst, plast-pfirst);
				int counter = 1;
				bool out_of_bound = false;
				while ( true ) {
					if ( maxbuf[plast-pfirst-counter] != onebyte ) {
						maxbuf[plast-pfirst-counter] = maxbuf[plast-pfirst-counter] + 1;
						break;
					} else {
						maxbuf[plast-pfirst-counter] = '\0';
						++counter;
					}
					if ( plast-pfirst-counter < 0 ) {
						out_of_bound = true;
						break;
					}
				}
				memcpy(minmax[ltabnum].minbuf+lminmax[ltabnum].offset, rstr.c_str(), plast-pfirst);				
				if ( !out_of_bound ) {
					memcpy(minmax[ltabnum].maxbuf+lminmax[ltabnum].offset, rstr.c_str(), plast-pfirst);
				}
			}
			str = AbaxFixString(minmax[ltabnum].minbuf, keylen[ltabnum]);
			return 1;
		} else if ( 2 == coltype ) {
			// must be col like constant or constant like constant
			return 0;
		} else {
			if ( 2 == cmode && jagstrtold(lstr.c_str(), NULL) != jagstrtold(rstr.c_str(), NULL) ) return 0; 
			else if ( 1 == cmode && jagatoll(lstr.c_str()) != jagatoll(rstr.c_str()) ) return 0; 
			else if ( strcmp(lstr.c_str(), rstr.c_str()) != 0 ) return 0; 
			str = "1";
			return 1;
		}
	}
	// match
	else if ( _binaryOp == JAG_FUNC_MATCH ) {
		// client side does this
		//prt(("s2039 JAG_FUNC_MATCH  lstr=[%s]\n", lstr.c_str() ));
		//prt(("s2039 JAG_FUNC_MATCH  rstr=[%s] coltype=%d\n", rstr.c_str(), coltype ));
		if ( NULL == _reg ) {
			try {
			    _reg = new std::regex( rstr.c_str() );
			} catch ( std::regex_error e ) {
				str = "0";
				return 0;
			} catch ( ... ) {
				str = "0";
				return 0;
			}
		}
		if ( regex_match( lstr.c_str(), *_reg ) ) {
			// matches to true
			str = "1";
			return 1;
		} else {
			str = "0";
			return 0;
		}
	}
	// AND &&
	else if ( _binaryOp == JAG_LOGIC_AND ) {
		findAndBuffer( minmax, lminmax, rminmax, attrs, numTabs, numKeys );
		str = "1";
		return 1;
	}
	// OR ||
	else if ( _binaryOp == JAG_LOGIC_OR ) {
		findOrBuffer( minmax, lminmax, rminmax, keylen, numTabs );
		str = "1";
		return 1;
	}
	
	// for other functions, if column exists, return 0
	if ( coltype > 0 ) {
		//prt(("s0883 coltype=%d > 0 return 0\n", coltype ));
		return 0;
	}
	abaxint num = 0; abaxdouble dnum = 0.0;
	// prt(("s0393 tmp9999  *************** skip never here????\n" ));
	
	// +
	if ( _binaryOp == JAG_NUM_ADD ) {
		prt(("s1928 JAG_NUM_ADD cmode=%d\n", cmode ));
		if ( 2 == cmode ) {
			dnum = jagstrtold(lstr.c_str(), NULL) + jagstrtold(rstr.c_str(), NULL);
			str = longDoubleToStr(dnum);
		} else if ( 1 == cmode ) {
			num = jagatoll(lstr.c_str()) + jagatoll(rstr.c_str());
			str = longToStr(num);
		}
		return 1;
	}	
	// - 
	else if ( _binaryOp == JAG_NUM_SUB ) {
		if ( 2 == cmode ) {
			dnum = jagstrtold(lstr.c_str(), NULL) - jagstrtold(rstr.c_str(), NULL);
			str = longDoubleToStr(dnum);
		} else if ( 1 == cmode ) {
			num = jagatoll(lstr.c_str()) - jagatoll(rstr.c_str());
			str = longToStr(num);
		}
		return 1;
	}	
	// *
	else if ( _binaryOp == JAG_NUM_MULT ) {
		if ( 2 == cmode ) {
			dnum = jagstrtold(lstr.c_str(), NULL) * jagstrtold(rstr.c_str(), NULL);
			str = longDoubleToStr(dnum);
		} else if ( 1 == cmode ) {
			num = jagatoll(lstr.c_str()) * jagatoll(rstr.c_str());
			str = longToStr(num);
		}
		return 1;
	}	
	// /
	else if ( _binaryOp == JAG_NUM_DIV ) {
		if ( 2 == cmode ) {
			if ( jagstrtold(rstr.c_str(), NULL) == 0.0 ) return -1;
			dnum = jagstrtold(lstr.c_str(), NULL) / jagstrtold(rstr.c_str(), NULL);
			str = longDoubleToStr(dnum);
		} else if ( 1 == cmode ) {
			if ( jagatoll(rstr.c_str()) == 0 ) return -1;
			num = jagatoll(lstr.c_str()) / jagatoll(rstr.c_str());
			str = longToStr(num);
		}
		return 1;
	}
	// %
	else if ( _binaryOp == JAG_NUM_REM ) {
		if ( jagatoll(rstr.c_str()) == 0 ) return -1;
		num = jagatoll(lstr.c_str()) / jagatoll(rstr.c_str());
		str = longToStr(num);
		return 1;
	}	
	// ^
	else if ( _binaryOp == JAG_NUM_POW || _binaryOp == JAG_FUNC_POW ) {
		if ( 2 == cmode ) {
			dnum = pow(jagstrtold(lstr.c_str(), NULL), jagstrtold(rstr.c_str(), NULL));
			str = longDoubleToStr(dnum);
		} else if ( 1 == cmode ) {
			num = pow(jagatoll(lstr.c_str()), jagatoll(rstr.c_str()));
			str = longToStr(num);
		}
		return 1;
	}
	// MOD
	else if ( _binaryOp == JAG_FUNC_MOD ) {
		dnum = fmod(jagstrtold(lstr.c_str(), NULL), jagstrtold(rstr.c_str(), NULL));
		str = longDoubleToStr(dnum);
		// prt(("s9301 JAG_FUNC_MOD calc dnum=%f lstr=[%s] rstr=[%s]\n", dnum, lstr.c_str(), rstr.c_str() ));
		return 1;
	}
	// LENGTH
	else if ( _binaryOp == JAG_FUNC_LENGTH ) {
		prt(("s2029 not called JAG_FUNC_LENGTH cmode=%d lstr=[%s]\n", cmode, lstr.c_str() ));
		if ( 2 == cmode ) {
			str = longDoubleToStr(jagstrtold(lstr.c_str(), NULL));
		} else if ( 1 == cmode ) {
			str = longToStr(jagatoll(lstr.c_str()));
		}
		return 1;
	}
	// ABS
	else if ( _binaryOp == JAG_FUNC_ABS ) {
		if ( 1 == cmode ) {
			str = longToStr(labs(jagatoll(lstr.c_str())));
		} else {
			str = longDoubleToStr(fabs(jagstrtold(lstr.c_str(), NULL)));
		}
		return 1;
	}
	// ACOS
	else if ( _binaryOp == JAG_FUNC_ACOS ) {
		str = longDoubleToStr(acos(jagstrtold(lstr.c_str(), NULL)));
		return 1;
	}	
	// ASIN
	else if ( _binaryOp == JAG_FUNC_ASIN ) {
		str = longDoubleToStr(asin(jagstrtold(lstr.c_str(), NULL)));
		return 1;
	}	
	// ATAN
	else if ( _binaryOp == JAG_FUNC_ATAN ) {
		str = longDoubleToStr(atan(jagstrtold(lstr.c_str(), NULL)));
		return 1;
	}
	// DIFF
	else if ( _binaryOp == JAG_FUNC_STRDIFF ) {
		//prt(("s2031 JAG_FUNC_STRDIFF lstr=[%s]\n", lstr.c_str() ));
		//prt(("s2031 JAG_FUNC_STRDIFF rstr=[%s]\n", rstr.c_str() ));
		int diff = levenshtein( lstr.c_str(), rstr.c_str() );
		//prt(("s2031 JAG_FUNC_STRDIFF diff=%d\n", diff ));
		str = intToStr( diff );
		return 1;
	}		
	// COS
	else if ( _binaryOp == JAG_FUNC_COS ) {
		str = longDoubleToStr(cos(jagstrtold(lstr.c_str(), NULL)));
		return 1;
	}		
	// SIN
	else if ( _binaryOp == JAG_FUNC_SIN ) {
		// not getting called in select cols nor in where
		str = longDoubleToStr(sin(jagstrtold(lstr.c_str(), NULL)));
		// prt(("s293938  tmp9999 JAG_FUNC_SIN str=[%s]\n", str.c_str() ));
		return 1;		
	}
	// TAN
	else if ( _binaryOp == JAG_FUNC_TAN ) {
		str = longDoubleToStr(tan(jagstrtold(lstr.c_str(), NULL)));
		return 1;
	}	
	// COT
	else if ( _binaryOp == JAG_FUNC_COT ) {
		if ( tan(jagstrtold(lstr.c_str(), NULL)) == 0 ) return -1;
		str = longDoubleToStr(1.0/tan(jagstrtold(lstr.c_str(), NULL)));
		return 1;
	}	
	// ALL
	else if ( _binaryOp == JAG_FUNC_ALL ) {
		// prt(("s7823 JAG_FUNC_ALL \n" ));
		//str = "ALL:ALL";
		str = "";
		return 1;
	}
	// LOG2
	else if ( _binaryOp == JAG_FUNC_LOG2 ) {
		str = longDoubleToStr(log2(jagstrtold(lstr.c_str(), NULL)));
		return 1;
	}	
	// LOG10
	else if ( _binaryOp == JAG_FUNC_LOG10 ) {
		str = longDoubleToStr(log10(jagstrtold(lstr.c_str(), NULL)));
		return 1;
	}	
	// LOG
	else if ( _binaryOp == JAG_FUNC_LOG ) {
		str = longDoubleToStr(log(jagstrtold(lstr.c_str(), NULL)));
		return 1;
	}
	// DEGREE
	else if ( _binaryOp == JAG_FUNC_DEGREES ) {
		str = longDoubleToStr(jagstrtold(lstr.c_str(), NULL)*57.295779513);
		return 1;
	}
	// RADIAN
	else if ( _binaryOp == JAG_FUNC_RADIANS ) {
		str = longDoubleToStr(jagstrtold(lstr.c_str(), NULL)/57.295779513);
		return 1;
	}
	// SQRT
	else if ( _binaryOp == JAG_FUNC_SQRT ) {
		str = longDoubleToStr(sqrt(jagstrtold(lstr.c_str(), NULL)));
		return 1;
	}
	// CEIL
	else if ( _binaryOp == JAG_FUNC_CEIL ) {
		if ( 1 == cmode ) {
			str = longToStr(ceil(jagatoll(lstr.c_str())));
		} else {
			str = longDoubleToStr(ceil(jagstrtold(lstr.c_str(), NULL)));
		}		
		return 1;
	}	
	// FLOOR
	else if ( _binaryOp == JAG_FUNC_FLOOR ) {
		if ( 1 == cmode ) {
			str = longToStr(floor(jagatoll(lstr.c_str())));
		} else {
			str = longDoubleToStr(floor(jagstrtold(lstr.c_str(), NULL)));
		}		
		return 1;
	}	
	// UPPER
	else if ( _binaryOp == JAG_FUNC_UPPER ) {
		str = makeUpperOrLowerFixString( lstr, 1 );
		return 1;
	}	
	// LOWER
	else if ( _binaryOp == JAG_FUNC_LOWER ) {
		str = makeUpperOrLowerFixString( lstr, 0 );
		return 1;
	}	
	// LTRIM
	else if ( _binaryOp == JAG_FUNC_LTRIM ) {
		str = lstr;
		str.ltrim();
		return 1;
	}	
	// RTRIM
	else if ( _binaryOp == JAG_FUNC_RTRIM ) {
		str = lstr;
		str.rtrim();
		return 1;
	}	
	// TRIM
	else if ( _binaryOp == JAG_FUNC_TRIM ) {
		str = lstr;
		str.trim();
		return 1;
	}
	// date, time, datetime etc
	else if ( isTimedateOp(_binaryOp) ) {
		str = JagTime::getValueFromTimeOrDate( _jpa, lstr, rstr, _binaryOp, _carg1 );
		return 1;
	}
	// SUBSTR
	else if ( _binaryOp == JAG_FUNC_SUBSTR ) {
		str = lstr;
		if ( _carg1.size() < 1 ) {
			str.substr( _arg1-1, _arg2 );
		} else {
			JagStrSplit sp( _carg1, '.' );
			Jstr encode = sp[sp.length()-1];
			JagLang lang;
			abaxint n = lang.parse( str.c_str(), encode.c_str() );
			if ( n < 0 ) {
				str.substr( _arg1-1, _arg2 );
			} else {
				lang.rangeFixString( str.size(), _arg1-1, _arg2, str );
			}
		}
		prt(("s4081 JAG_FUNC_SUBSTR str=[%s]\n", str.c_str() ));
		return 1;
	}
	// MILETOMETER
	else if ( _binaryOp == JAG_FUNC_MILETOMETER ) {
		str = _carg1;
		return 1;
	}
	// TOSECOND
	else if ( _binaryOp == JAG_FUNC_TOSECOND ) {
		str = _carg1;
		//prt(("s3082 JAG_FUNC_TOSECOND _carg1=[%s] str=[%s]\n", _carg1.c_str(), str.c_str() ));
		return 1;
	}
	// TOMICROSECOND
	else if ( _binaryOp == JAG_FUNC_TOMICROSECOND ) {
		str = _carg1;
		return 1;
	}
	// CURDATE, CURTIME, NOW with no child funcs
	else if ( _binaryOp == JAG_FUNC_CURDATE || _binaryOp == JAG_FUNC_CURTIME || _binaryOp == JAG_FUNC_NOW ) {
		short collen = getFuncLength( _binaryOp );
		struct tm res; time_t now = time(NULL);
		now -= (_jpa.servtimediff - _jpa.timediff) * 60;
		jag_localtime_r( &now, &res );
		char *buf = (char*) jagmalloc ( collen+1 );
		memset( buf, 0,  collen+1 );
		if ( _binaryOp == JAG_FUNC_CURDATE ) {
			//sprintf( buf, "%4d-%02d-%02d", res.tm_year+1900, res.tm_mon+1, res.tm_mday );
			strftime( buf, collen+1, "%Y-%m-%d", &res );
		} else if ( _binaryOp == JAG_FUNC_CURTIME ) {
			//sprintf( buf, "%02d:%02d:%02d", res.tm_hour, res.tm_min, res.tm_sec );
			strftime( buf, collen+1, "%H:%M:%S", &res );
		} else if ( _binaryOp == JAG_FUNC_NOW ) {
			//sprintf( buf, "%4d-%02d-%02d %02d:%02d:%02d", 
				//res.tm_year+1900, res.tm_mon+1, res.tm_mday,  res.tm_hour, res.tm_min, res.tm_sec );
			strftime( buf, collen+1, "%Y-%m-%d %H:%M:%S", &res );
		}
		str = AbaxFixString( buf, collen );
		if ( buf ) free ( buf );
		return 1;
	}
	else if ( _binaryOp == JAG_FUNC_TIME ) {
		char buf[32];
		sprintf( buf, "%ld", time(NULL) );
		str = AbaxFixString( buf );
		return 1;
	} else if ( _binaryOp == JAG_FUNC_PI ) {
		char buf[32];
		sprintf( buf, "%f", JAG_PI );
		str = AbaxFixString( buf );
		return 1;
	}
	// DISTANCE
	else if ( _binaryOp == JAG_FUNC_DISTANCE ) {
		// never reached???
		str = "1010.23";
		//prt(("s3312 never ??? BinaryOpNode::doWhereCalc lstr=[%s] rstr=[%s]\n", lstr.c_str(), rstr.c_str() ));
		return 1;
	}
	// ... more calcuations ...
	
	//prt(("s4028 _binaryOp=%d return -1\n", _binaryOp ));
	return -1;
}

// cmode 0: regard as string; cmode 1: regard as int; cmode 2: regard as double/float
// return -1: error; 0, 1 OK
int BinaryOpNode::_doCalculation( AbaxFixString &lstr, AbaxFixString &rstr, 
	int &ltmode, int &rtmode,  const Jstr& ltype,  const Jstr& rtype, 
	int llength, int rlength, bool &first )
{
	prt(("s0398 enter BinaryOpNode::_doCalculation lstr=[%s] first=%d\n", lstr.c_str(), first ));
	prt(("s0398 enter BinaryOpNode::_doCalculation rstr=[%s] first=%d\n", rstr.c_str(), first ));
	prt(("s0398 enter BinaryOpNode::_doCalculation ltype=[%s] rtype=[%s]\n", ltype.c_str(), rtype.c_str() ));

	int cmode;
	if ( ltmode > rtmode ) cmode = ltmode;
	else cmode = rtmode;

	prt(("s0398 enter BinaryOpNode::_doCalculation _binaryOp=%d (%s) cmode=%d\n", _binaryOp, binaryOpStr(_binaryOp).c_str(), cmode ));

	Jstr errmsg;
	// first, check if need to change datetime format
	if ( _right && rstr.size()>0 && 0!=strncmp(rstr.c_str(), "CJAG", 6) ) {
		if ( isDateTime(ltype) && 0 == rtype.size() ) {
			// left is date time column and right is constant, covert right
			char buf[llength+1];
			memset( buf, 0, llength+1 );
			formatOneCol( _jpa.timediff, _jpa.servtimediff, buf, rstr.c_str(), errmsg, "GARBAGE", 0, llength, 0, ltype );
			rstr = AbaxFixString( buf, llength );
		} else if ( isDateTime(rtype) && 0 == ltype.size() ) {
			// right is date time column and left is constant, covert left
			char buf[rlength+1];
			memset( buf, 0, rlength+1 );
			formatOneCol( _jpa.timediff, _jpa.servtimediff, buf, lstr.c_str(), errmsg, "GARBAGE", 0, rlength, 0, rtype );
			lstr = AbaxFixString( buf, rlength );
		} // otherwise, do nothing
	}
	
	// begin process calculations
	// aggregate funcs 
	// MIN
	if ( _binaryOp == JAG_FUNC_MIN ) {
		if ( 0 == cmode ) {
			if ( first ) {
				return 1;
			} else if ( memcmp(lstr.c_str(), _opString.c_str(), _opString.length()) < 0 ) {
				return 1;
			} else {
				// no error, ignore
				lstr = _opString;
				return 1;
			}
		} else if ( 1 == cmode ) {
			if ( first ) {
				lstr = longToStr(jagatoll(lstr.c_str()));
				return 1;
			} else if ( jagatoll(lstr.c_str()) < jagatoll(_opString.c_str()) ) {
				lstr = longToStr(jagatoll(lstr.c_str()));
				return 1;
			} else {
				// no error, ignore
				lstr = _opString;
				return 1;
			}			
		} else {			
			if ( first ) {
				lstr = longDoubleToStr(jagstrtold(lstr.c_str(), NULL));
				return 1;
			} else if ( jagstrtold(lstr.c_str(), NULL) < jagstrtold(_opString.c_str(), NULL) ) {
				lstr = longDoubleToStr(jagstrtold(lstr.c_str(), NULL));
				return 1;
			} else {
				// no error, ignore
				lstr = _opString;
				return 1;
			}
		}
	}	
	// MAX
	else if ( _binaryOp == JAG_FUNC_MAX ) {
		if ( 0 == cmode ) {
			if ( first ) {
				return 1;
			} else if ( memcmp(lstr.c_str(), _opString.c_str(), _opString.length()) > 0 ) {
				return 1;
			} else {
				// no error, ignore
				lstr = _opString;
				return 1;
			}
		} else if ( 1 == cmode ) {
			if ( first ) {
				lstr = longToStr(jagatoll(lstr.c_str()));
				return 1;
			} else if ( jagatoll(lstr.c_str()) > jagatoll(_opString.c_str()) ) {
				lstr = longToStr(jagatoll(lstr.c_str()));
				return 1;
			} else {
				// no error, ignore
				lstr = _opString;
				return 1;
			}			
		} else {			
			if ( first ) {
				lstr = longDoubleToStr(jagstrtold(lstr.c_str(), NULL));
				return 1;
			} else if ( jagstrtold(lstr.c_str(), NULL) > jagstrtold(_opString.c_str(), NULL) ) {
				lstr = longDoubleToStr(jagstrtold(lstr.c_str(), NULL));
				return 1;
			} else {
				// no error, ignore
				lstr = _opString;
				return 1;
			}
		}		
	}	
	// AVG
	else if ( _binaryOp == JAG_FUNC_AVG ) {
		// always regard as double
		++_numCnts;
		abaxdouble avg = jagstrtold(_opString.c_str(), NULL);
		avg += (jagstrtold(lstr.c_str(), NULL)-avg)*(1.0/_numCnts);
		lstr = longDoubleToStr(avg);
		ltmode = 2;
		return 1;
	}	
	// SUM
	else if ( _binaryOp == JAG_FUNC_SUM ) {
		if ( 1 == cmode ) {
			lstr = longToStr(jagatoll(lstr.c_str())+jagatoll(_opString.c_str()));
			return 1;
		} else {
			lstr = longDoubleToStr(jagstrtold(lstr.c_str(), NULL)+jagstrtold(_opString.c_str(), NULL));
			return 1;
		}
	}
	// COUNT
	else if ( _binaryOp == JAG_FUNC_COUNT ) {
		lstr = longToStr(jagatoll(_opString.c_str())+1);
		return 1;
	}
	// STDDEV
	else if ( _binaryOp == JAG_FUNC_STDDEV ) {
		// always regard as double
		++_numCnts;
		abaxdouble stdev = jagstrtold(lstr.c_str(), NULL);
		if ( first ) {
			_initK = stdev;
			lstr = "0";
		} else {
			_stddevSum += stdev-_initK;
			_stddevSumSqr += (stdev-_initK)*(stdev-_initK);
			lstr = longDoubleToStr(sqrt((_stddevSumSqr-(_stddevSum*_stddevSum)/_numCnts)/_numCnts));
		}
		ltmode = 2;
		return 1;
	}	
	// FIRST
	else if ( _binaryOp == JAG_FUNC_FIRST ) {
		if ( first ) {
		} else {
			// no error, ignore
			lstr = _opString;
		}
		return 1;
	}	
	// LAST
	else if ( _binaryOp == JAG_FUNC_LAST ) {
		return 1;
	}
	
	
	// non aggregate funcs
	// = ==
	if ( _binaryOp == JAG_FUNC_EQUAL ) {
		// prt(("s5983 tmp9999  _binaryOp == JAG_FUNC_EQUAL ...\n" ));
		if ((0 == cmode && strcmp(lstr.c_str(), rstr.c_str()) == 0) || 
			(1 == cmode && jagatoll(lstr.c_str()) == jagatoll(rstr.c_str())) ||
			(2 == cmode && jagstrtold(lstr.c_str(), NULL) == jagstrtold(rstr.c_str(), NULL))) {
			lstr = "1";
			// prt(("s0034 tmp9999  lstr=1 return 1 cmode=%d\n", cmode ));
			return 1;
		} else {
			lstr = "0";
			// prt(("s0035 tmp9999  lstr=0 return 0 cmode=%d\n", cmode ));
			return 0;
		}
	}
	// != <> ><
	else if ( _binaryOp == JAG_FUNC_NOTEQUAL ) {
		if ((0 == cmode && strcmp(lstr.c_str(), rstr.c_str()) != 0) ||
			(1 == cmode && jagatoll(lstr.c_str()) != jagatoll(rstr.c_str())) ||
			(2 == cmode && jagstrtold(lstr.c_str(), NULL) != jagstrtold(rstr.c_str(), NULL))) {
			lstr = "1";
			return 1;
		} else {
			lstr = "0";
			return 0;
		}
	}
	// < || <=
	else if ( _binaryOp == JAG_FUNC_LESSTHAN ) {
		//prt(("s3048 JAG_FUNC_LESSTHAN cmode=%d\n", cmode ));
		//prt(("s3048 lstr=[%s] rstr=[%s]\n", lstr.c_str(), rstr.c_str() ));

		if ((0 == cmode && strcmp(lstr.c_str(), rstr.c_str()) < 0) ||
			(1 == cmode && jagatoll(lstr.c_str()) < jagatoll(rstr.c_str())) ||
			(2 == cmode && jagstrtold(lstr.c_str(), NULL) < jagstrtold(rstr.c_str(), NULL))) {
			lstr = "1";
			return 1;
		} else {
			lstr = "0";
			return 0;
		}
	} else if ( _binaryOp == JAG_FUNC_LESSEQUAL ) {
		if ((0 == cmode && strcmp(lstr.c_str(), rstr.c_str()) <= 0) ||
			(1 == cmode && jagatoll(lstr.c_str()) <= jagatoll(rstr.c_str())) ||
			(2 == cmode && jagstrtold(lstr.c_str(), NULL) <= jagstrtold(rstr.c_str(), NULL))) {
			lstr = "1";
			return 1;
		} else {
			lstr = "0";
			return 0;
		}	
	}
	// > || >=
	else if ( _binaryOp == JAG_FUNC_GREATERTHAN ) {
		if ((0 == cmode && strcmp(lstr.c_str(), rstr.c_str()) > 0) ||
			(1 == cmode && jagatoll(lstr.c_str()) > jagatoll(rstr.c_str())) ||
			(2 == cmode && jagstrtold(lstr.c_str(), NULL) > jagstrtold(rstr.c_str(), NULL))) {
			lstr = "1";
			return 1;
		} else {
			lstr = "0";
			return 0;
		}	
	} else if ( _binaryOp == JAG_FUNC_GREATEREQUAL ) {
		if ((0 == cmode && strcmp(lstr.c_str(), rstr.c_str()) >= 0) ||
			(1 == cmode && jagatoll(lstr.c_str()) >= jagatoll(rstr.c_str())) ||
			(2 == cmode && jagstrtold(lstr.c_str(), NULL) >= jagstrtold(rstr.c_str(), NULL))) {
			lstr = "1";
			return 1;
		} else {
			lstr = "0";
			return 0;
		}		
	}
	// like
	else if ( _binaryOp == JAG_FUNC_LIKE ) {
		if ( cmode > 0 || ltype != JAG_C_COL_TYPE_STR ) {
			// left must be column, and right can be either column or constant
			lstr = "";
			return -1;
		}
		char *pfirst = (char*)rstr.c_str();
		char *plast = strrchr( (char*)rstr.c_str(), '%');
		if ( rtype.size() > 0 || !plast ) {
			// right is column or no '%' in like string, like applied as EQ
			if ( strcmp(lstr.c_str(), rstr.c_str()) == 0 ) {
				lstr = "1";
				return 1;
			} else {
				lstr = "0";
				return 0;
			}
		} else {
			// right is constant and has '%' char in like string 
			if ( *pfirst != '%' && memcmp(lstr.c_str(), pfirst, plast-pfirst) == 0 ) {
				lstr = "1";
				return 1;
			} else if ( *pfirst == '%' && plast != pfirst ) {
				*plast = '\0';
				if ( strstr(lstr.c_str(), pfirst+1) ) {
					*plast = '%';
					lstr = "1";
					return 1;
				} else {
					*plast = '%';
					lstr = "0";
					return 0;
				}
			} else if ( *pfirst == '%' && plast == pfirst ) {
				if ( lastStrEqual(lstr.c_str(), pfirst+1, lstr.length(), strlen(pfirst+1)) ) {
					lstr = "1";
					return 1;
				} else {
					lstr = "0";
					return 0;
				}
			} else {
				lstr = "";
				return -1;
			}
		}
	}
	// match
	else if ( _binaryOp == JAG_FUNC_MATCH ) {
		// server side does this
		//prt(("s2239 JAG_FUNC_MATCH  lstr=[%s]\n", lstr.c_str() ));
		//prt(("s2239 JAG_FUNC_MATCH  rstr=[%s] cmode=%d\n", rstr.c_str(), cmode ));
		if ( NULL == _reg ) {
			try {
			    _reg = new std::regex( rstr.c_str() );
			} catch ( std::regex_error e ) {
				lstr = "0";
				return 0;
			} catch ( ... ) {
				lstr = "0";
				return 0;
			}
		}
		if ( regex_match( lstr.c_str(), *_reg ) ) {
			lstr = "1";
			return 1;
		} else {
			lstr = "0";
			return 0;
		}
	}
	// +
	else if ( _binaryOp == JAG_NUM_ADD ) {
		prt(("s9923 JAG_NUM_ADD cmode=%d lstr=%s rstr=%s\n", cmode, lstr.c_str(), rstr.c_str()));
		if ( 2 == cmode ) {
			lstr = longDoubleToStr(jagstrtold(lstr.c_str(), NULL)+jagstrtold(rstr.c_str(), NULL));
			return 1;
		} else if ( 1 == cmode ) {		
			lstr = longToStr(jagatoll(lstr.c_str())+jagatoll(rstr.c_str()));
			return 1;
		} else {
			// string addition, concat
			lstr = lstr.concat(rstr);
			return 1;
		}
	}	
	// - 
	else if ( _binaryOp == JAG_NUM_SUB ) {
		if ( 1 == cmode ) {
			lstr = longToStr(jagatoll(lstr.c_str())-jagatoll(rstr.c_str()));
			return 1;
		} else {			
			lstr = longDoubleToStr(jagstrtold(lstr.c_str(), NULL)-jagstrtold(rstr.c_str(), NULL));
			return 1;
		}
	}	
	// *
	else if ( _binaryOp == JAG_NUM_MULT ) {
		if ( 1 == cmode ) {
			lstr = longToStr(jagatoll(lstr.c_str())*jagatoll(rstr.c_str()));
			return 1;
		} else {
			lstr = longDoubleToStr(jagstrtold(lstr.c_str(), NULL)*jagstrtold(rstr.c_str(), NULL));
			return 1;
		}	
	}	
	// /
	else if ( _binaryOp == JAG_NUM_DIV ) {
		if ( 1 == cmode ) {
			if ( jagatoll(rstr.c_str()) == 0 ) {
				lstr = "";
				return -1;
			} else {
				lstr = longToStr(jagatoll(lstr.c_str())/jagatoll(rstr.c_str()));
				return 1;
			}
		} else {
			if ( jagstrtold(rstr.c_str(), NULL) == 0 ) {
				lstr = "";
				return -1;
			} else {
				lstr = longDoubleToStr(jagstrtold(lstr.c_str(), NULL)/jagstrtold(rstr.c_str(), NULL));
				return 1;
			}
		}
	}
	// %
	else if ( _binaryOp == JAG_NUM_REM ) {
		// for operator %, can only use integer type instead of double type
		if ( jagatoll(rstr.c_str()) == 0 ) {
			lstr = "";
			return -1;
		} else {
			lstr = longToStr(jagatoll(lstr.c_str())%atoll(rstr.c_str()));
			return 1;
		}
	}	
	// ^
	else if ( _binaryOp == JAG_NUM_POW || _binaryOp == JAG_FUNC_POW ) {
		if ( 1 == cmode ) {
			lstr = longToStr(pow(jagatoll(lstr.c_str()), jagatoll(rstr.c_str())));
			return 1;
		} else {
			lstr = longDoubleToStr(pow(jagstrtold(lstr.c_str(), NULL), jagstrtold(rstr.c_str(), NULL)));
			return 1;
		}
	}
	// MOD
	else if ( _binaryOp == JAG_FUNC_MOD ) {
		// always regard as double
		lstr = longDoubleToStr(fmod(jagstrtold(lstr.c_str(), NULL), jagstrtold(rstr.c_str(), NULL)));
		ltmode = 2;
		//prt(("s6039 tmp9999 JAG_FUNC_MOD  lstr=[%s]\n", lstr.c_str() ));
		return 1;
	}
	// LENGTH
	else if ( _binaryOp == JAG_FUNC_LENGTH ) {
    	if ( 2 == cmode ) {
    		lstr = longDoubleToStr(jagstrtold(lstr.c_str(), NULL));
    	} else if ( 1 == cmode ) {
    		lstr = longToStr(jagatoll(lstr.c_str()));
    	} 
    	lstr = longToStr( strlen(lstr.c_str()) );
		return 1;
	}
	// LINELENGTH
	else if ( _binaryOp == JAG_FUNC_LINELENGTH ) {
		prt(("s2429 called. JAG_FUNC_LENGTH cmode=%d lstr=[%s] lstr.length=%d\n", cmode, lstr.c_str(), lstr.length() ));
		lstr = doubleToStr(JagGeo::getGeoLength( lstr ));
		prt(("s2384 cjag lstr=%s\n", lstr.c_str() ));
		return 1;
	}
	// ABS
	else if ( _binaryOp == JAG_FUNC_ABS ) {
		if ( 1 == cmode ) {
			lstr = longToStr(labs(jagatoll(lstr.c_str())));
		} else {
			lstr = longDoubleToStr(fabs(jagstrtold(lstr.c_str(), NULL)));
		}
		return 1;
	}
	// ACOS
	else if ( _binaryOp == JAG_FUNC_ACOS ) {
		// always regard as double
		lstr = longDoubleToStr(acos(jagstrtold(lstr.c_str(), NULL)));
		ltmode = 2;
		return 1;
	}	
	// ASIN
	else if ( _binaryOp == JAG_FUNC_ASIN ) {
		// always regard as double
		lstr = longDoubleToStr(asin(jagstrtold(lstr.c_str(), NULL)));
		ltmode = 2;
		return 1;
	}	
	// ATAN
	else if ( _binaryOp == JAG_FUNC_ATAN ) {
		// always regard as double
		lstr = longDoubleToStr(atan(jagstrtold(lstr.c_str(), NULL)));
		ltmode = 2;
		return 1;
	}
	// DIFF
	else if ( _binaryOp == JAG_FUNC_STRDIFF ) {
		//ltmode = 0; // string
		ltmode = 1; // string
		//prt(("s2431 JAG_FUNC_STRDIFF lstr=[%s]\n", lstr.c_str() ));
		//prt(("s2431 JAG_FUNC_STRDIFF rstr=[%s]\n", rstr.c_str() ));
		int diff = levenshtein( lstr.c_str(), rstr.c_str() );
		//prt(("s2431 JAG_FUNC_STRDIFF diff=%d\n", diff ));
		lstr = intToStr( diff );
		return 1;
	}		
	// COS
	else if ( _binaryOp == JAG_FUNC_COS ) {
		// always regard as double
		lstr = longDoubleToStr(cos(jagstrtold(lstr.c_str(), NULL)));
		ltmode = 2;
		return 1;
	}		
	// SIN
	else if ( _binaryOp == JAG_FUNC_SIN ) {
		// always regard as double
		// called in select sin(a) from t;
		lstr = longDoubleToStr(sin(jagstrtold(lstr.c_str(), NULL)));
		// prt(("s5039 tmp9999 JAG_FUNC_SIN lstr=[%s]\n", lstr.c_str() ));
		ltmode = 2;
		return 1;		
	}
	// TAN
	else if ( _binaryOp == JAG_FUNC_TAN ) {
		// always regard as double
		lstr = longDoubleToStr(tan(jagstrtold(lstr.c_str(), NULL)));
		ltmode = 2;
		return 1;
	}	
	// COT
	else if ( _binaryOp == JAG_FUNC_COT ) {
		// always regard as double
		if ( tan(jagstrtold(lstr.c_str(), NULL)) == 0 ) {
			lstr = "";
			return -1;
		} else {
			lstr = longDoubleToStr(1.0/tan(jagstrtold(lstr.c_str(), NULL)));
			ltmode = 2;
			return 1;
		}
	}	
	// ALL
	else if ( _binaryOp == JAG_FUNC_ALL ) {
		//lstr = "ALL:ALL";
		//prt(("s7824 JAG_FUNC_ALL \n" ));
		// lstr = "999.876";
		//prt(("s2831 lstr=[%s]\n", lstr.c_str() ));
		//prt(("s2832 rstr=[%s]\n", rstr.c_str() ));
		//lstr = "";
		ltmode = 0;
		//return 1;
		if ( lstr.size() > 0 ) {
			return 1;
		} else {
			return 0;
		}
		//return 0;
	}
	// LOG2
	else if ( _binaryOp == JAG_FUNC_LOG2 ) {
		// always regard as double
		lstr = longDoubleToStr(log2(jagstrtold(lstr.c_str(), NULL)));
		ltmode = 2;
		return 1;
	}	
	// LOG10
	else if ( _binaryOp == JAG_FUNC_LOG10 ) {
		// always regard as double
		lstr = longDoubleToStr(log10(jagstrtold(lstr.c_str(), NULL)));
		ltmode = 2;
		return 1;
	}	
	// LOG
	else if ( _binaryOp == JAG_FUNC_LOG ) {
		// always regard as double
		lstr = longDoubleToStr(log(jagstrtold(lstr.c_str(), NULL)));
		ltmode = 2;
		return 1;
	}
	// DEGREE
	else if ( _binaryOp == JAG_FUNC_DEGREES ) {
		// always regard as double
		lstr = longDoubleToStr(jagstrtold(lstr.c_str(), NULL)*57.295779513);
		ltmode = 2;
		return 1;
	}
	// RADIAN
	else if ( _binaryOp == JAG_FUNC_RADIANS ) {
		// always regard as double
		lstr = longDoubleToStr(jagstrtold(lstr.c_str(), NULL)/57.295779513);
		ltmode = 2;
		return 1;
	}
	// SQRT
	else if ( _binaryOp == JAG_FUNC_SQRT ) {
		// always regard as double
		lstr = longDoubleToStr(sqrt(jagstrtold(lstr.c_str(), NULL)));
		ltmode = 2;
		return 1;
	}
	// CEIL
	else if ( _binaryOp == JAG_FUNC_CEIL ) {
		if ( 1 == cmode ) {
			lstr = longToStr(ceil(jagatoll(lstr.c_str())));
		} else {
			lstr = longDoubleToStr(ceil(jagstrtold(lstr.c_str(), NULL)));
		}		
		return 1;
	}	
	// FLOOR
	else if ( _binaryOp == JAG_FUNC_FLOOR ) {
		if ( 1 == cmode ) {
			lstr = longToStr(floor(jagatoll(lstr.c_str())));
		} else {
			lstr = longDoubleToStr(floor(jagstrtold(lstr.c_str(), NULL)));
		}		
		return 1;
	}	
	// UPPER
	else if ( _binaryOp == JAG_FUNC_UPPER ) {
		lstr = makeUpperOrLowerFixString( lstr, 1 );
		return 1;
	}	
	// LOWER
	else if ( _binaryOp == JAG_FUNC_LOWER ) {
		lstr = makeUpperOrLowerFixString( lstr, 0 );
		return 1;
	}	
	// LTRIM
	else if ( _binaryOp == JAG_FUNC_LTRIM ) {
		lstr.ltrim();
		return 1;
	}	
	// RTRIM
	else if ( _binaryOp == JAG_FUNC_RTRIM ) {
		lstr.rtrim();
		return 1;
	}	
	// TRIM
	else if ( _binaryOp == JAG_FUNC_TRIM ) {
		lstr.trim();
		return 1;
	}
	// date, time, datetime etc
	else if ( isTimedateOp(_binaryOp) ) {
		lstr = JagTime::getValueFromTimeOrDate( _jpa, lstr, rstr, _binaryOp, _carg1 );
		return 1;
	}
	// SUBSTR
	else if ( _binaryOp == JAG_FUNC_SUBSTR ) {
		if ( _carg1.size() < 1 ) {
			lstr.substr( _arg1-1, _arg2 );
		} else {
			JagStrSplit sp( _carg1, '.' );
			Jstr encode = sp[sp.length()-1];
			JagLang lang;
			abaxint n = lang.parse( lstr.c_str(), encode.c_str() );
			if ( n < 0 ) {
				lstr.substr( _arg1-1, _arg2 );
			} else {
				lang.rangeFixString( lstr.size(), _arg1-1, _arg2, lstr );
			}
		}
		//prt(("s2330 JAG_FUNC_SUBSTR lstr=[%s]\n", lstr.c_str() ));
		return 1;
	} else if ( _binaryOp == JAG_FUNC_TOSECOND ) {
		//prt(("s4008 JAG_FUNC_TOSECOND _carg1=[%s]\n", _carg1.c_str() ));
		lstr = _carg1;
		return 1;
	} else if ( _binaryOp == JAG_FUNC_TOMICROSECOND  ) {
		//prt(("s4008 JAG_FUNC_TOSECOND _carg1=[%s]\n", _carg1.c_str() ));
		lstr = _carg1;
		return 1;
	} else if ( _binaryOp == JAG_FUNC_MILETOMETER ) {
		lstr = _carg1;
		return 1;
    }
	// CURDATE, CURTIME, NOW with no child funcs
	else if ( _binaryOp == JAG_FUNC_CURDATE || _binaryOp == JAG_FUNC_CURTIME || _binaryOp == JAG_FUNC_NOW ) {
		short collen = getFuncLength( _binaryOp );
		struct tm res; time_t now = time(NULL);
		now -= (_jpa.servtimediff - _jpa.timediff) * 60;
		jag_localtime_r( &now, &res );
		char *buf = (char*) jagmalloc ( collen+1 );
		memset( buf, 0,  collen+1 );
		if ( _binaryOp == JAG_FUNC_CURDATE ) {
			//sprintf( buf, "%4d-%02d-%02d", res.tm_year+1900, res.tm_mon+1, res.tm_mday );
			strftime( buf, collen+1, "%Y-%m-%d", &res );
		} else if ( _binaryOp == JAG_FUNC_CURTIME ) {
			//sprintf( buf, "%02d:%02d:%02d", res.tm_hour, res.tm_min, res.tm_sec );
			strftime( buf, collen+1, "%H:%M:%S", &res );
		} else if ( _binaryOp == JAG_FUNC_NOW ) {
			//sprintf( buf, "%4d-%02d-%02d %02d:%02d:%02d", 
				//res.tm_year+1900, res.tm_mon+1, res.tm_mday,  res.tm_hour, res.tm_min, res.tm_sec );
			strftime( buf, collen+1, "%Y-%m-%d %H:%M:%S", &res );
		}
		lstr = AbaxFixString( buf, collen );
		if ( buf ) free ( buf );
		return 1;
	} else if ( _binaryOp == JAG_FUNC_TIME ) {
		char buf[32];
		sprintf( buf, "%ld", time(NULL) );
		lstr = AbaxFixString( buf );
		return 1;
	} else if ( _binaryOp == JAG_FUNC_PI ) {
		char buf[32];
		sprintf( buf, "%f", JAG_PI );
		lstr = AbaxFixString( buf );
		ltmode = 2;
		return 1;
	} else if ( _binaryOp == JAG_FUNC_DISTANCE ) {
		// always regard as double
		//prt(("s5480 do JAG_FUNC_DISTANCE lstr=[%s]\n", lstr.c_str() ));
		//prt(("s5480 do JAG_FUNC_DISTANCE rstr=[%s]\n", rstr.c_str() ));
		//prt(("s5480 do JAG_FUNC_DISTANCE _carg1=[%s]\n", _carg1.c_str() ));
		double dist;
		if ( ! JagGeo::distance( lstr, rstr, _carg1, dist ) ) {
			return 0;
		}
		lstr = doubleToStr( dist );
		ltmode = 2;
		return 1;
	} else if ( _binaryOp == JAG_FUNC_AREA  ||  _binaryOp == JAG_FUNC_VOLUME 
				|| _binaryOp == JAG_FUNC_PERIMETER 
			    || _binaryOp == JAG_FUNC_XMIN || _binaryOp == JAG_FUNC_YMIN || _binaryOp == JAG_FUNC_ZMIN
	            || _binaryOp == JAG_FUNC_XMAX || _binaryOp == JAG_FUNC_YMAX || _binaryOp == JAG_FUNC_ZMAX ) {
		ltmode = 2; // double
		bool brc = false;
		double val = 0.0;
		prt(("s4086 processSingleDoubleOp lstr=[%s] ...\n", lstr.c_str() ));
		try {
			brc = processSingleDoubleOp( _binaryOp, lstr, _carg1, val );
		} catch ( int e ) {
			brc = false;
		} catch ( ... ) {
			brc = false;
		}

		if ( brc ) {
			lstr = doubleToStr( val );
			return 1;
		} else {
			lstr = "0";
			return 0;
		}
	} else if ( _binaryOp == JAG_FUNC_POINTN || _binaryOp == JAG_FUNC_BBOX || _binaryOp == JAG_FUNC_STARTPOINT
				 || _binaryOp == JAG_FUNC_CONVEXHULL || _binaryOp == JAG_FUNC_BUFFER 
				 || _binaryOp == JAG_FUNC_TOPOLYGON  || _binaryOp == JAG_FUNC_ASTEXT
				 || _binaryOp == JAG_FUNC_CENTROID ||  _binaryOp == JAG_FUNC_OUTERRING || _binaryOp == JAG_FUNC_OUTERRINGS
				 ||  _binaryOp == JAG_FUNC_INNERRINGS || _binaryOp == JAG_FUNC_RINGN || _binaryOp == JAG_FUNC_INNERRINGN
	             || _binaryOp == JAG_FUNC_ENDPOINT || _binaryOp == JAG_FUNC_ISCLOSED
	             || _binaryOp == JAG_FUNC_ISSIMPLE  || _binaryOp == JAG_FUNC_ISRING
	             || _binaryOp == JAG_FUNC_ISCONVEX  || _binaryOp == JAG_FUNC_INTERPOLATE
				 || _binaryOp == JAG_FUNC_LINESUBSTRING || _binaryOp == JAG_FUNC_REMOVEPOINT
	             || _binaryOp == JAG_FUNC_ISVALID  || _binaryOp == JAG_FUNC_ISPOLYGONCCW 
				 || _binaryOp == JAG_FUNC_ISPOLYGONCW || _binaryOp == JAG_FUNC_POLYGONN 
				 || _binaryOp == JAG_FUNC_SRID || _binaryOp == JAG_FUNC_SUMMARY
				 || _binaryOp == JAG_FUNC_NUMSEGMENTS  || _binaryOp == JAG_FUNC_NUMINNERRINGS
				 ||  _binaryOp == JAG_FUNC_NUMPOLYGONS || _binaryOp == JAG_FUNC_UNIQUE
				 ||  _binaryOp == JAG_FUNC_REVERSE || _binaryOp == JAG_FUNC_UNIQUE
				 ||  _binaryOp == JAG_FUNC_SCALE ||  _binaryOp == JAG_FUNC_SCALESIZE
				 || _binaryOp == JAG_FUNC_TRANSLATE || _binaryOp == JAG_FUNC_TRANSSCALE
				 || _binaryOp == JAG_FUNC_ROTATE || _binaryOp == JAG_FUNC_ROTATEAT || _binaryOp == JAG_FUNC_ROTATESELF
				 || _binaryOp == JAG_FUNC_AFFINE
				 || _binaryOp == JAG_FUNC_NUMPOINTS || _binaryOp == JAG_FUNC_NUMRINGS ) {
		//ltmode = 0; // string
		ltmode = getTypeMode( _binaryOp );

		bool brc = false;
		Jstr val;
		prt(("s4081 processSingleStrOp lstr=[%s] ...\n", lstr.c_str() ));
		prt(("s4082 processSingleStrOp _carg1=[%s] ...\n", _carg1.c_str() ));
		try {
			brc = processSingleStrOp( _binaryOp, lstr, _carg1, val );
		} catch ( int e ) {
			brc = false;
		} catch ( ... ) {
			brc = false;
		}

		//prt(("s1928 processSingleStrOp brc=%d val=[%s]\n", brc, val.c_str() ));
		if ( brc ) {
			lstr = val;
			return 1;
		} else {
			lstr = "";
			return 0;
		}
	} else if ( _binaryOp == JAG_FUNC_DIMENSION ) {
		ltmode = 1; // int
		if (  _left && _left->_isElement ) {
			// prt(("s0292 left->_srid=%d left->_name=%s left->_type=[%s]\n", _left->_srid, _left->_name.c_str(), _left->_type.c_str() ));
			lstr = intToStr(JagGeo::getPolyDimension( _left->_type ) );
		} else {
			lstr = "0";
		}
		return 1;
	} else if ( _binaryOp == JAG_FUNC_GEOTYPE ) {
		ltmode = 0; // string
		if (  _left && _left->_isElement ) {
			lstr = JagGeo::getTypeStr( _left->_type );
		} else {
			lstr = "Primitive";
		}
		return 1;
	} else if ( _binaryOp == JAG_FUNC_WITHIN || _binaryOp == JAG_FUNC_COVEREDBY
	            || _binaryOp == JAG_FUNC_CONTAIN || _binaryOp == JAG_FUNC_COVER 
	            || _binaryOp == JAG_FUNC_SAME
	            || _binaryOp == JAG_FUNC_NEARBY 
				|| _binaryOp == JAG_FUNC_INTERSECT || _binaryOp == JAG_FUNC_DISJOINT ) {
		// always regard as int  boolean
		ltmode = 1; // int
		bool brc;
		//prt(("s4003 _carg1=[%s]\n", _carg1.c_str() ));
		prt(("s7350 before processBooleanOp lstr=[%s]\n", lstr.c_str() ));
		prt(("s7350 before processBooleanOp rstr=[%s]\n", rstr.c_str() ));
		try {
			brc = processBooleanOp( _binaryOp, lstr, rstr, _carg1 );
		} catch ( int e ) {
			brc = 0;
		} catch ( ... ) {
			brc = 0;
		}

		if ( brc ) {
			lstr = "1";
			return 1;
		} else {
			lstr = "0";
			return 0;
		}
	} else if (  _binaryOp == JAG_FUNC_UNION || _binaryOp == JAG_FUNC_COLLECT ||  _binaryOp == JAG_FUNC_SYMDIFFERENCE
				 || _binaryOp == JAG_FUNC_CLOSESTPOINT ||  _binaryOp == JAG_FUNC_ANGLE
				 || _binaryOp == JAG_FUNC_SCALEAT
				 || _binaryOp == JAG_FUNC_LOCATEPOINT || _binaryOp == JAG_FUNC_ADDPOINT || _binaryOp == JAG_FUNC_SETPOINT
	             || _binaryOp == JAG_FUNC_INTERSECTION || _binaryOp == JAG_FUNC_DIFFERENCE ) {
		prt(("s7140 before processTwoStrOp lstr=[%s]\n", lstr.c_str() ));
		prt(("s7140 before processTwoStrOp rstr=[%s]\n", rstr.c_str() ));
		ltmode = getTypeMode( _binaryOp );
		try {
			lstr = processTwoStrOp( _binaryOp, lstr, rstr, _carg1 );
		} catch ( ... ) {
			lstr = "";
		}
		return 1;
	}
	// ... more calcuations ...
	
	lstr = "";
	return -1;
}

////////////// BinaryExpressionBuilder ////////////////
BinaryExpressionBuilder::BinaryExpressionBuilder()
{
	//prt(("s8612 BinaryExpressionBuilder ctor called\n" ));
}

BinaryExpressionBuilder::~BinaryExpressionBuilder()
{
	//prt(("s8613 BinaryExpressionBuilder dtor called\n" ));
}

void BinaryExpressionBuilder::init( const JagParseAttribute &jpa, JagParseParam *pparam )
{
	_jpa = jpa; _datediffClause = _substrClause = -1; _isNot = 0; 
	_lastIsOperand = 0;
	_pparam = pparam;
}

void BinaryExpressionBuilder::clean()
{
	_jpa.clean();
	operatorStack.clean();
	operandStack.clean();
}

// BinaryExpressionBuilder methods for all
ExprElementNode* BinaryExpressionBuilder::getRoot() const
{
	// prt(("s3901 BinaryExpressionBuilder::getRoot() operandStack.size()=%d %0x\n", operandStack.size(), this ));
	if ( operandStack.size() == 0 ) {
		return NULL;
	}

    ExprElementNode *topp = operandStack.top();
    // return static_cast<BinaryOpNode *> (topp);
    return topp;
}

// Input: +, -, *, /, %, ^, =, !=, >, < and other functions
// creates BinaryOpNode's from al preceding
// type 0: default accept all
// type 1: "where/join-on" tree, not accept stand-alone ',' and aggregate funcs
// type 2: "update" tree, not accept aggregate funcs
BinaryOpNode* BinaryExpressionBuilder::parse( const JagParser *jagParser, const char* str, int type,
		const JagHashMap<AbaxString, AbaxPair<AbaxString, abaxint>> &cmap, JagHashStrInt &jmap, 
		Jstr &colList )
{
	//prt(("s3712 BinaryExpressionBuilder::parse() str=[%s] type=%d\n", str, type ));
	//Jstr columns = JagParser::getColumns( str );
	//prt(("s2821 getColumns=[%s]\n", columns.c_str() ));
	if ( 0 == strncasecmp(str, "all(", 4 ) ) {
	} else if (  0 == strncasecmp(str, "geotype(", 8 ) ) {
		// _GEOTYPE
		type = 0;
	} else if (  0 == strncasecmp(str, "pointn(", 7 ) ) {
		// _POINTN
		type = 0;
	} else if (  0 == strncasecmp(str, "bbox(", 5 ) || 0 == strncasecmp(str, "envelope(", 9 ) ) {
		// _BBOX
		type = 0;
	} else if (  0 == strncasecmp(str, "startpoint(", 11 ) ) {
		// _STARTPOINT 
		type = 0;
	} else if (  0 == strncasecmp(str, "endpoint(", 9 )
			  ) {
		// _ENDPOINT  
		type = 0;
	}
	//prt(("s2822 parse columns=[%s]\n", columns.c_str() ));

    const char *p = str, *q;
	short fop = 0, len = 0, isandor = 0, ctype = 0;
	StringElementNode lastNode;
	//lastNode._columns = columns;
	
	while ( *p != '\0' ) {
		while ( isspace(*p) ) ++p;
		//prt(("s0231 p=[%s] isandor=%d\n", p, isandor ));
		if ( *p == ',' ) {
			if ( _isNot ) throw 2101;
			if ( 1 == type ) {
				// no ',' in where tree
				// throw 2; // disable for distance(a,b) in where
			}
			++p;
			_lastIsOperand = 0;
		} else if ( *p == '(' ) {
			if ( _isNot ) throw 2100;
			if ( _datediffClause >= 0 || _substrClause >= 0 ) throw 2184;
			prt(("s9011 operatorStack.push (\n" ));
            operatorStack.push('(');
			_lastOp ='(';
            ++p;
			isandor = 0;
			_lastIsOperand = 0;
        } else if ( *p == ')' ) {
			if ( _isNot ) throw 2105;
			if ( isandor ) {
				// no right parenthesis or "and/or" followed just after and/or
				throw 2136;
			}
			//prt(("s9012 processRightParenthesis \n" ));
            processRightParenthesis( jmap );
            ++p;
			isandor = 0;
			if ( _datediffClause >= 0 ) {
				if ( _datediffClause != 3 ) throw 99;
				_datediffClause = -1;
			} else if ( _substrClause >= 0 ) {
				if ( _substrClause < 3 || _substrClause > 4 ) throw 98;
				_substrClause = -1;
			}
			_lastIsOperand = 1;
		} else if ( 0 == strncasecmp( p, "and ", 4 ) ) {
			//prt(("s3870 p=[%s] isandor=%d _lastIsOperand=%d\n", p, isandor, _lastIsOperand ));
			if ( _isNot ) throw 2227;
			if ( _datediffClause >= 0 || _substrClause >= 0 ) throw 2128;
			if ( !_lastIsOperand ) throw 2180;
			// "and" condition
			if ( isandor ) { throw 2189; }
			processOperator( JAG_LOGIC_AND, jmap );
			p += 4;
			isandor = 1;
			_lastIsOperand = 0;
		} else if ( 0 == strncasecmp( p, "&& ", 3 ) ) {
			if ( _isNot ) throw 10;
			if ( _datediffClause >= 0 || _substrClause >= 0 ) throw 11;
			if ( !_lastIsOperand ) throw 2181;
			// alias of "and"
			if ( isandor ) {
				throw 2112;
			}
			processOperator( JAG_LOGIC_AND, jmap );
			p += 3;
			isandor = 1;
			_lastIsOperand = 0;
        } else if ( 0 == strncasecmp( p, "or ", 3 ) ) {
			if ( _isNot ) throw 13;
			if ( _datediffClause >= 0 || _substrClause >= 0 ) throw 2114;
			if ( !_lastIsOperand ) throw 2182;
			// "or" condition
            if ( isandor ) {
                throw 2115;
            }
            processOperator( JAG_LOGIC_OR, jmap );
            p += 2;
            isandor = 1;
			_lastIsOperand = 0;
		} else if ( 0 == strncasecmp( p, "|| ", 3 ) ) {
			if ( _isNot ) throw 16;
			if ( _datediffClause >= 0 || _substrClause >= 0 ) throw 2117;
			if ( !_lastIsOperand ) throw 2183;
			// alias of "or"
			if ( isandor ) {
				throw 2118;
			}
			processOperator( JAG_LOGIC_OR, jmap );
			p += 3;
			isandor = 1;
			_lastIsOperand = 0;
		} else if ( 0 == strncasecmp( p, "between ", 8 ) ) {
			if ( _datediffClause >= 0 || _substrClause >= 0 ) throw 2119;
			if ( !_lastIsOperand ) throw 2184;
			processBetween(jagParser, p, q, lastNode, cmap, jmap, colList );
			_lastIsOperand = 1;
			isandor = 0; // fixed bug
		} else if ( 0 == strncasecmp( p, "in", 2 ) && ( isspace(*(p+2)) || *(p+2) == '(') ) {
			if ( _datediffClause >= 0 || _substrClause >= 0 ) throw 2120;
			if ( !_lastIsOperand ) throw 2185;
			processIn( jagParser, p, q, lastNode, cmap, jmap, colList );
			_lastIsOperand = 1;
			isandor = 0; 
		} else if ( getCalculationType( p, fop, len, ctype ) ) {
			//prt(("s9203 p=[%s] ctype=%d\n", p, ctype ));
			if ( 0 == ctype ) {
				if ( !_lastIsOperand ) {
					// special case, if last is not operand, check to make sure if current is negative value or invalid
					if ( isdigit(*(p+1)) ) {
						// regard as negative number
						if ( _isNot ) throw 2188;
						processOperand( jagParser, p, q, lastNode, cmap, colList );
						_lastIsOperand = 1;
						continue;
					}
					throw 2286;
				}
				// + - * / % math op
				if ( _isNot ) throw 2221;
				processOperator( *p, jmap );
			} else if ( 3 == ctype ) {
				if ( !_lastIsOperand ) throw 2287;
				// set "not" flag, may used for following "between ... and ..." or " in(..." syntax 
				if ( _isNot ) throw 2222;
				_isNot = 1;
			} else {
				// other function op and = != > >= etc.
				if ( _isNot ) throw 2223;
				// "where/join" tree and "update" tree not accept aggregate funcs
				//prt(("s3381 fop=%d type=%d\n", fop, type ));
				if ( BinaryOpNode::isAggregateOp( fop ) && type != 0 ) {
					//prt(("s8811 throw 2224\n" ));
					throw 2224;
				}
				prt(("s2039 operatorStack.push(%d)\n", fop ));
				operatorStack.push(fop);
				_lastOp = fop;
			}
			p += len;
			isandor = 0;
			_lastIsOperand = 0;
		} else {
			if ( _isNot ) throw 2225;
			//prt(("s3063 processOperand p=[%s]\n", p ));
			processOperand(jagParser, p, q, lastNode, cmap, colList );
			_lastIsOperand = 1;
		}
	}

    while (!operatorStack.empty()) {
		//prt(("s2030 doBinary( operator.top )\n" ));
        doBinary( operatorStack.top(), jmap );
        operatorStack.pop();
    }

    // Invariant: At this point the operandStack should have only one element
    // operandStack.size() == 1
    // otherwise, the expression is not well formed.
    if ( operandStack.size() != 1 ) {
		prt(("s2093 operandStack.size=%d !=1  throw 2326\n", operandStack.size() ));
        throw 2326;
    }

    ExprElementNode *topp = operandStack.top();
    return static_cast<BinaryOpNode *> (topp);
}

// p begin with "between "
void BinaryExpressionBuilder::processBetween( const JagParser *jpsr, const char *&p, const char *&q, StringElementNode &lastNode,
				const JagHashMap<AbaxString, AbaxPair<AbaxString, abaxint>> &cmap, JagHashStrInt &jmap, 
				Jstr &colList )
{
	StringElementNode lnode( this, lastNode._name, lastNode._value, _jpa, 
							 lastNode._tabnum, lastNode._typeMode );
	// process operator >=, if "isNot", process operator <
	if ( !_isNot ) processOperator( JAG_FUNC_GREATEREQUAL, jmap );
	else processOperator( JAG_FUNC_LESSTHAN, jmap );
	// get first operand after between
	p += 7;
	while ( isspace(*p) ) ++p;
	processOperand( jpsr,  p, q, lastNode, cmap, colList );
	// process LOGIC_AND
	while ( isspace(*p) ) ++p;
	if ( 0 != strncasecmp( p, "and", 3 ) || *(p+3) != ' ' ) throw 2327;
	p += 3;
	while ( isspace(*p) ) ++p;
	// and process LOGIC_AND, if "isNot", process LOGIC_OR
	if ( !_isNot ) processOperator( JAG_LOGIC_AND, jmap );
	else processOperator( JAG_LOGIC_OR, jmap );
	// get second operand after and
	// push lnode to operand stack again, and get second operand
	StringElementNode *newNode = new StringElementNode( this, lastNode._name, lastNode._value, 
														_jpa, lastNode._tabnum, lastNode._typeMode );

    prt(("s4440 new element, operandStack.push( newelement node )\n"));
	operandStack.push(newNode);
	// and process operator <=, if "isNot", process operator >
	if ( !_isNot ) processOperator( JAG_FUNC_LESSEQUAL, jmap );
	else processOperator( JAG_FUNC_GREATERTHAN, jmap );
	processOperand( jpsr, p, q, lastNode, cmap, colList );
	if ( _isNot ) _isNot = 0;
}

// p begin with "in " or "in("
void BinaryExpressionBuilder::processIn( const JagParser *jpsr, const char *&p, const char *&q, StringElementNode &lastNode,
										 const JagHashMap<AbaxString, AbaxPair<AbaxString, abaxint>> &cmap, JagHashStrInt &jmap, 
										 Jstr &colList )
{
	StringElementNode lnode( this, lastNode._name, lastNode._value, _jpa, lastNode._tabnum, lastNode._typeMode );
	bool first = 1, hasSep = 0;
	p += 2;
	while ( isspace(*p) ) ++p;
	if ( *p != '(' ) throw 28;
	++p;
	while ( 1 ) {
		while ( isspace(*p) ) ++p;
		if ( *p == ')' || *p == '\0' ) break;
		else if ( *p == ',' ) {
			if ( first ) throw 29;
			++p;
			if ( !_isNot ) processOperator( JAG_LOGIC_OR, jmap );
			else processOperator( JAG_LOGIC_AND, jmap );
			hasSep = 1;
			continue;
		}

		if ( !first ) {	
			StringElementNode *newNode = new StringElementNode( this, lastNode._name, lastNode._value, 
													_jpa, lastNode._tabnum, lastNode._typeMode );
			operandStack.push(newNode);
		} else {
			first = 0;
		}

		if ( !_isNot ) processOperator( JAG_FUNC_EQUAL, jmap );
		else processOperator( JAG_FUNC_NOTEQUAL, jmap );
		processOperand( jpsr, p, q, lastNode, cmap, colList );
		hasSep = 0;
	}
	
	if ( *p == '\0' || hasSep == 1 ) throw 30;
	++p;
	if ( _isNot ) _isNot = 0;
}

// for operand, if inside quote ( ' or " ), must be constant
// otherwise, if all bytes are digits, must be constant
// type mode for string, integer or float mode: 0, 1, 2
void BinaryExpressionBuilder::processOperand( const JagParser *jpsr, const char *&p, const char *&q, StringElementNode &lastNode,
	const JagHashMap<AbaxString, AbaxPair<AbaxString, abaxint>> &cmap, Jstr &colList )
{
	Jstr name, value;
	int typeMode = 1; // init as integer
	const char *r;
	q = p;

	//prt(("\ns3830 enter processOperand p=[%s]\n", p ));
	/**
	// debug only
	StringElementNode *topp = (StringElementNode*)getRoot();
	prt(("s0299 root tree: %0x\n", topp ));
	if ( topp ) { 
		topp->print(0);
		prt(("\n"));
	} else {
		prt(("s2033 root is NULL\n" ));
	}
	**/
	
	if ( *p == '\'' || *p == '"' ) {
		// quote operand is constant
		if ( (q = jumptoEndQuote(q)) && *q == '\0' ) {
			// no end quote found, error
			throw 31;
		}
		for ( r = p+1; r < q; ++r ) {
			if ( !isdigit(*r) && *r != '.' && *r != '+' && *r != '-' ) {
				typeMode = 0;
			} else if ( *r == '.' ) {
				typeMode = 2;
			}
		}
		typeMode = 0;
		r = p+1;
		value = Jstr(r, q-r);
		StringElementNode *newNode = new StringElementNode( this, name, value, _jpa, 0, typeMode );
		//prt(("s2931 operandStack.push name=%s value=%s\n", name.c_str(), value.c_str() ));
		operandStack.push(newNode);
		// prt(("s0393  new StringElementNode name=[%s] value=[%s] typeMode=%d\n", name.c_str(), value.c_str(), typeMode ));
		if ( _substrClause >= 0 ) ++_substrClause;
		else if ( _datediffClause >= 0 ) ++_datediffClause;
	} else if ( 0 == strncasecmp(p, "point(", 6 ) 
				|| 0==strncasecmp(p, "point3d(", 8 ) 
				|| 0==strncasecmp(p, "circle(", 7 ) 
				|| 0==strncasecmp(p, "circle3d(", 9 ) 
				|| 0==strncasecmp(p, "sphere(", 7 ) 
				|| 0==strncasecmp(p, "square(", 7 ) 
				|| 0==strncasecmp(p, "square3d(", 9 ) 
				|| 0==strncasecmp(p, "cube(", 5 ) 
				|| 0==strncasecmp(p, "rectangle(", 10 ) 
				|| 0==strncasecmp(p, "rectangle3d(", 12 ) 
				|| 0==strncasecmp(p, "box(", 4 ) 
				|| 0==strncasecmp(p, "cone(", 5 ) 
				|| 0==strncasecmp(p, "line(", 5 ) 
				|| 0==strncasecmp(p, "line3d(", 7 ) 
				|| 0==strncasecmp(p, "triangle(", 9 ) 
				|| 0==strncasecmp(p, "triangle3d(", 11 ) 
				|| 0==strncasecmp(p, "cylinder(", 9 ) 
				|| 0==strncasecmp(p, "ellipse(", 8 ) 
				|| 0==strncasecmp(p, "ellipse3d(", 10 ) 
				|| 0==strncasecmp(p, "ellipsoid(", 10 ) 
				|| 0==strncasecmp(p, "linestring(", 11 ) 
				|| 0==strncasecmp(p, "linestring3d(", 13 ) 
				|| 0==strncasecmp(p, "multipoint(", 11 ) 
				|| 0==strncasecmp(p, "multipoint3d(", 13 ) 
				|| 0==strncasecmp(p, "polygon(", 8 ) 
				|| 0==strncasecmp(p, "polygon3d(", 10 ) 
				|| 0==strncasecmp(p, "multilinestring(", 16 ) 
				|| 0==strncasecmp(p, "multilinestring3d(", 18 ) 
				|| 0==strncasecmp(p, "multipolygon(", 13 ) 
				|| 0==strncasecmp(p, "multipolygon3d(", 15 ) 
				|| 0==strncasecmp(p, "range(", 6 ) 
	          ) {
		// point(1 2), pt:y )  _POINT or _POINT3D
		/***
		while ( *p != '(' ) ++p;
		++p;  // (p  .. .. .. )
		prt(("s8820 p=[%s] q=[%s]\n", p, q ));
		// prt(("s8821 p=[%s] q=[%s]\n", p, q ));
		q = strchr(p, ')' );
		if ( ! q ) { throw 101; }
		// (p xxxxxxx )  q points to )
		**/
		bool isPoly = false;
		if ( 0==strncasecmp(p, "linestring", 10 ) || 0==strncasecmp(p, "polygon", 7) 
			 || 0==strncasecmp(p, "multipoint", 10 ) || 0==strncasecmp(p, "multilinestring", 12 ) 
			 || 0==strncasecmp(p, "multipolygon", 10 )
		   ) { isPoly = true; }

		q = p;
		while ( *q != '(' ) ++q;
		Jstr geotype(p, q-p); // "point" or "sphere" etc
		Jstr typeStr = geotype;
		geotype = JagGeo::convertType2Short( geotype ); // to PT, SP, etc, short type
		//prt(("s3481 geotype=[%s]\n", geotype.c_str() ));
		//prt(("s3481 p=[%s] q=[%s]\n", p, q ));
		
		Jstr val;
		bool isRaw = false;
		if ( isPoly ) {
			if ( geotype == JAG_C_COL_TYPE_POLYGON || geotype == JAG_C_COL_TYPE_POLYGON3D 
				 || geotype == JAG_C_COL_TYPE_MULTILINESTRING || geotype == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
				p = q; // ( points to (
				prt(("s3482 p=[%s] q=[%s]\n", p, q ));
				q = strrchr(p, ')' );
				if ( ! q ) { throw 101; }
				char *d = strdup(p);
				JagParser::removeEndUnevenBracketAll( d );
				val = d;
				free(d );
				prt(("s6752 val=[%s]\n", val.c_str() ));
				q = p + val.size();
				isRaw = true;
			} else {
				//prt(("s3484 p=[%s] q=[%s]\n", p, q ));
				p = q+1; // (p
				//prt(("s3485 p=[%s] q=[%s]\n", p, q ));
				q = strchr(p, ')' );
				if ( ! q ) { throw 102; }
				//prt(("s3486 p=[%s] q=[%s]\n", p, q ));
				val = Jstr(p, q-p);
				//prt(("s6753 val=[%s]\n", val.c_str() ));
    			JagStrSplit sp(val, ',' );
    			Jstr c, newval;
    			for ( int i=0; i < sp.length(); ++i ) {
    				JagStrSplit cd(sp[i], ' ', true );
    				if ( cd.length() == 2 ) {
    					c = cd[0] + ":" + cd[1];
    				} else if ( cd.length() == 3 ) {
    					c = cd[0] + ":" + cd[1] + ":" + cd[2];
    				} else {
    					c = sp[i];
    				}
    
    				if ( newval.size() < 1 ) {
    					newval = c;
    				} else {
    					newval += Jstr(" ") + c;
    				}
    			}
    			val = newval;
			}
		} else {
			//std::replace(val.begin(), val.end(), ',', ' ');
			//val = replaceChar(val, ',', ' ');
			p = q+1; // (p
			//prt(("s3485 p=[%s] q=[%s]\n", p, q ));
			q = strchr(p, ')' );
			if ( ! q ) { throw 102; }
			////prt(("s3486 p=[%s] q=[%s]\n", p, q ));
			val = Jstr(p, q-p);
			if ( geotype != JAG_C_COL_TYPE_RANGE ) {
				val.replace( ',', ' ');
			}
		}
		//prt(("s6753 val=[%s]\n", val.c_str() ));

		name = "";
		//prt(("s8830 p=[%s] q=[%s]\n", p, q ));
		if ( geotype == JAG_C_COL_TYPE_RANGE ) {
			//prt(("s6754 val=[%s]\n", val.c_str() ));
			JagStrSplit sp( val, ',');
			//prt(("s2239 sp.length()=%d\n", sp.length() ));
			if ( sp.length() < 2 ) {
				throw 2319;
			}
			sp[0].remove('\'');
			sp[0].trimSpaces(2);
			sp[0].replace(' ', '_');
			sp[1].remove('\'');
			sp[1].trimSpaces(2);
			sp[1].replace(' ', '_');
			val = sp[0] + "|" + sp[1];
		}

		// value = Jstr("CJAG=0=0=") + geotype + "=0 " + val;
		if ( isRaw ) {
			value = typeStr + val;
			//prt(("s3627 isRaw value=%s\n", value.c_str() ));
		} else {
			value = Jstr("CJAG=0=0=") + geotype + "=d 0:0:0:0 " + val;
			//prt(("s3628 !isRaw value=%s\n", value.c_str() ));
		}

		typeMode = 2;
		//prt(("s2739 name=[] value=[%s]\n", value.c_str() ));
		// value is inside ( )  point(22 33 4 44)  "22 33 4 44" is saved in value. name is empty
		StringElementNode *newNode = new StringElementNode( this, name, value, _jpa, 0, typeMode );
		//prt(("s4502 in procOpernd new StrElemNode nme=[%s] val=[%s] opedStk.push \n", name.c_str(), value.c_str() ));
		operandStack.push(newNode);
		++q; p = q;
	} else {
		//prt(("s6028 processOperand q=[%s]\n", q ));
		// if ( !isalnum(*q) && *q != '_' && *q != '-' && *q != '+' && *q != '.' ) throw 32;
		if ( !isValidNameChar(*q) && *q != '_' && *q != '-' && *q != '+' && *q != '.' && *q != ':' ) throw 32;
		++q;
		//prt(("s6029 q=[%s]\n", q ));
		//while ( isalnum(*q) || *q == '_' || *q == '.' ) ++q;
		while ( isValidNameChar(*q) || *q == '_' || *q == '.' || *q == ':' ) ++q;
		//prt(("s6029 processOperand q=[%s]\n", q ));
		r = p;
		for ( r = p; r < q; ++r ) {
			if ( !isdigit(*r) && *r != '.' && *r != '-' && *r != '+' ) {
				typeMode = 0;
			} else if ( *r == '.' ) {
				typeMode = 2;
			}
		}

		if ( typeMode > 0 ) {
			// for no quote operand, if all digits, regard as constant
			value = Jstr(p, q-p);
			StringElementNode *newNode = new StringElementNode( this, name, value, _jpa, 0, typeMode );
		    prt(("s4503 in processOperand new StringElementNode name=[%s] value=[%s] operandStack.push \n", name.c_str(), value.c_str() ));
			operandStack.push(newNode);
			//operandStack.print();
			if ( _substrClause >= 0 ) ++_substrClause;
			else if ( _datediffClause >= 0 ) ++_datediffClause;
		} else {
			// otherwise, if is first part of datediff or the fourth part of substr, regard as value
			if ( 3 == _substrClause || 0 == _datediffClause ) {
				value = Jstr(p, q-p);
				StringElementNode *newNode = new StringElementNode( this, name, value, _jpa, 0, typeMode );
				// prt(("s5503 processOperand new StringElementNode name=[%s] value=[%s]\n", name.c_str(), value.c_str() ));
				operandStack.push(newNode);
				if ( _substrClause >= 0 ) ++_substrClause;
				else if ( _datediffClause >= 0 ) ++_datediffClause;
			} else {				
				// otherwise, must be regard as column name
				int tabnum = 0;
				name = Jstr(p, q-p);
				//name = makeLowerString( name );
				name.toLower();
				//prt(("s0291 name=[%s] colList=[%s]\n", name.c_str(), colList.c_str() ));
				//prt(("s0291 lastNode._name=[%s] lastNode._type=[%s]\n", lastNode._name.c_str(), lastNode._type.c_str() ));
				if ( !nameConvertionCheck( name, tabnum, cmap, colList ) ) {
					throw 303;
				}
				//prt(("s0292 after nameConvertionCheck name=[%s] colList=[%s]\n", name.c_str(), colList.c_str() ));

				if ( ! nameAndOpGood( jpsr, name, lastNode ) ) {
					throw 334;
				}

				StringElementNode *newNode = new StringElementNode( this, name, value, _jpa, tabnum, typeMode );
				prt(("s5505 processOperand new StringElementNode name=[%s] value=[%s] operandStack.push()\n", name.c_str(), value.c_str() ));
				// name="test.el1.c1:y"

				lastNode._name = newNode->_name;
				lastNode._value = newNode->_value;
				lastNode._tabnum = newNode->_tabnum;
				lastNode._typeMode = newNode->_typeMode;
				operandStack.push(newNode);

				if ( _substrClause >= 0 ) ++_substrClause;
				else if ( _datediffClause >= 0 ) ++_datediffClause;

			}
		}
	}

	if ( *p == '\'' || *p == '"' ) p = q+1;
	else p = q;

	/***
	prt(("s9393 end processoperand() p=[%s]\n\n", p ));
	topp = (StringElementNode*)getRoot();
	prt(("s0299 root tree: %0x\n", topp ));
	if ( topp ) { topp->print(0); prt(("\n")); }
	else prt(("s8039 root NULL\n" ));
	***/
}

// process +-*/%^ and or
void BinaryExpressionBuilder::processOperator( short op, JagHashStrInt &jmap )
{
    // pop operators with higher precedence and create their BinaryOpNode
    short opPrecedence = precedence( op );
	prt(("s3084 enter processOperator op=%d opPrecedence=%d\n", op, opPrecedence ));
    while ((!operatorStack.empty()) && (opPrecedence <= precedence( operatorStack.top() ))) {
		prt(("s2094 doBinary(%c) ...\n", operatorStack.top() ));
        doBinary( operatorStack.top(), jmap );
        operatorStack.pop();
    }

    // lastly push the operator passed onto the operatorStack
	prt(("s0931 operatorStack.push(%c)\n", op ));
    operatorStack.push(op);
	_lastOp = op;
	// prt(("s2930 processOperator _lastOp=[%d]\n", _lastOp ));

	/***
	// debug only
	prt(("s7208 processOperator:\n" ));
	//operatorStack.print();
	prt(("\n" ));
	***/
}

void BinaryExpressionBuilder::processRightParenthesis( JagHashStrInt &jmap )
{
	if ( operatorStack.empty() ) return;
	while ( 1 ) {
		int fop = operatorStack.top();
		if ( fop == '(' ) {
			//prt(("s8733 in processRightParenthesis see (, pop it and break\n" ));
			operatorStack.pop(); // remove '('
			break;
		} else if ( checkFuncType( fop ) ) {
			//prt(("s4093 procRightParent see fop=%d(%s) doBinary, operatorStack.pop(); break\n", fop, BinaryOpNode::binaryOpStr(fop).c_str() ));
			doBinary( fop, jmap );
			operatorStack.pop();
			break;
		} else {
			//prt(("s4094 in procRightParent see fop=%d doBinary, operatorStack.pop(); loop\n", fop));
			prt(("s2736 checkFuncType(%d) is not valid checkFuncType, doBinary ...\n", fop ));
			doBinary( fop, jmap );
			operatorStack.pop();
		}
	}
}

// Creates a BinaryOpNode from the top two operands on operandStack
// These top two operands are removed (poped), and the new BinaryOperation
// takes their place on the top of the stack.
void BinaryExpressionBuilder::doBinary( short op, JagHashStrInt &jmap )
{
	Jstr opstr = BinaryOpNode::binaryOpStr(op);
	//prt(("s3376 doBinary op=[%s]\n", opstr.c_str() ));

	ExprElementNode *right = NULL;
	ExprElementNode *left = NULL;
	int arg1 = 0, arg2 = 0;
	Jstr carg1;
	const char *p = NULL;;
	ExprElementNode *t3;
	if ( funcHasTwoChildren(op) ) {
    	if ( operandStack.empty() ) { throw 1434; } 
		int nargs = operandStackTopSize();
		if ( op == JAG_FUNC_NEARBY ) {
			t3 = operandStack.top();
			if ( t3->getValue(p) ) { carg1 = p; } else throw 1318;
			operandStack.pop();  // popped the third element
		} else if ( op == JAG_FUNC_DISTANCE ) {
			if ( 2 == nargs ) {
				carg1 = "center";  // default distance type
			} else {
				t3 = operandStack.top();
				if ( t3->getValue(p) ) { carg1 = p; } else throw 1319;
				operandStack.pop();  // popped the third element
			}
		} else if ( op == JAG_FUNC_ADDPOINT ) {
			// addpoint(lstr, point, [pos] )
			// removepoint(lstr, pos )
			if ( 2 == nargs ) {
				carg1 = "-1";  // default -1, meaning add point at end . 1-based. 
			} else {
				t3 = operandStack.top();
				if ( t3->getValue(p) ) { carg1 = p; } else throw 1319;
				operandStack.pop();  // popped the third element
			}
		} else if ( op == JAG_FUNC_SETPOINT ) {
			// setpoint(lstr, point, pos )
			// removepoint(lstr, pos )
			t3 = operandStack.top();
			if ( t3->getValue(p) ) { carg1 = p; } else throw 1329;
			operandStack.pop();  // popped the third element
		}  else if ( op == JAG_FUNC_SCALEAT ) {
			if (  nargs >= 3 ) {
				t3 = operandStack.top();
				if ( t3->getValue(p) ) { carg1 = p; } else throw 1332;
				operandStack.pop();  // popped the third element
			} 

			if ( nargs >= 4 ) {
				t3 = operandStack.top();
				if ( t3->getValue(p) ) { carg1 = Jstr(p) + ":" + carg1; } else throw 1340;
				operandStack.pop();  // popped the third element
			} 
			
			if ( nargs >= 5 ) {
				t3 = operandStack.top();
				if ( t3->getValue(p) ) { carg1 = Jstr(p) + ":" + carg1; } else throw 1341;
				operandStack.pop();  // popped the third element
			}
		}

		// right child
    	if ( operandStack.empty() ) { throw 1434; } 
    	right = operandStack.top();
    	operandStack.pop();
    
    	// left child
    	if ( operandStack.empty() ) { throw 1435; } 
    	left = operandStack.top();
    	operandStack.pop();

		// if datediff, pop and process third top operand as diff type
		if ( op == JAG_FUNC_DATEDIFF ) {
			// datediff(type, BEGIN_TIMECOL, END_TIMECOL )  type is first element, and last in stack
			ExprElementNode *tdiff = NULL;
			const char *p = NULL;
			if ( operandStack.empty() ) throw 436;
			else tdiff = operandStack.top();
			if ( tdiff->getValue(p) ) {
				carg1 = "m";
				if ( *p == 's' || *p == 'S' ) carg1 = "s";
				else if ( *p == 'h' || *p == 'H' ) carg1 = "h";
				else if ( *p == 'd' || *p == 'D' ) carg1 = "D";
				else if ( (*p == 'm' || *p == 'M') && ( *(p+1) == 'o' || *(p+1) == 'O' ) ) carg1 = "M";
				else if ( (*p == 'm' || *p == 'M') && ( *(p+1) == 'i' || *(p+1) == 'I' ) ) carg1 = "m";
				else if ( *p == 'y' || *p == 'Y' ) carg1 = "Y";
				else throw 437;				
			} else throw 438;
			operandStack.pop();
		}

		// for compare op, and each child is different table column, setup joinmap
		if ( BinaryOpNode::isCompareOp( op ) ) {
			const char *p, *q;
			abaxint lrc, rrc;
			lrc = left->getName(p);
			rrc = right->getName(q);
			if ( lrc >= 0 && rrc >= 0 && lrc != rrc ) {
				jmap.addKeyValue( p, lrc );
				jmap.addKeyValue( q, rrc );
			}
		}
	} else if ( funcHasOneConstant(op) ) {
		if ( operandStack.empty() ) throw 4306;
		if ( op == JAG_FUNC_TOSECOND || op == JAG_FUNC_TOMICROSECOND  ) {
			ExprElementNode *en = NULL;
			const char *p = NULL;
			en = operandStack.top();
			if ( en->getValue(p) ) {
				// here directly compute and assign result to _carg1
				//prt(("s6203 p=[%s]\n", p ));
				abaxint num;
				if ( op == JAG_FUNC_TOSECOND  ) {
					num = convertToSecond(p);
					if ( num < 0 ) throw 4316;
					carg1 = longToStr( num );
				} else {
					num = convertToMicroSecond(p);
					if ( num < 0 ) throw 4326;
					carg1 = longToStr( num );
				}
				//prt(("s6203 carg1=[%s]\n", carg1.c_str() ));
			} else throw 4308;
			operandStack.pop();
		} else if ( op == JAG_FUNC_MILETOMETER ) {
			ExprElementNode *en = NULL;
			const char *p = NULL;
			en = operandStack.top();
			if ( en->getValue(p) ) {
				double meter = jagatof( p ) * 1069.344;
				carg1 = doubleToStr( meter );
			} else throw 4309;
			operandStack.pop();
		} 
	} else if ( funcHasZeroChildren(op) ) {
		// nothing to be done
	} else {
		// process one column but may have some arguments
		// if substr, pop and process first two top operands as length and offset of substr
		if ( operandStack.empty() ) throw 3439;
		if ( op == JAG_FUNC_SUBSTR ) {
			ExprElementNode *targ = NULL;
			const char *p = NULL;
			int hasEncode = 0;
			targ = operandStack.top();
			if ( targ->getValue(p) ) {
				if ( isdigit(*p) ) { arg2 = jagatoi(p); }
				else { 
					hasEncode = 1;
					carg1 = p; 
				}
			} else { throw 3440; }
    		operandStack.pop();

    		if ( operandStack.empty() ) throw 3441;
    		targ = operandStack.top();
			if ( ! hasEncode ) {
    			if ( targ->getValue(p) ) {
    				if ( isdigit(*p) ) arg1 = jagatoi(p);
    			} else throw 3442;
			} else {
				if ( targ->getValue(p) ) {
					if ( isdigit(*p) ) { arg2 = jagatoi(p); }
				} else throw 3443;

    			operandStack.pop();
    			if ( operandStack.empty() ) throw 3444;
    			targ = operandStack.top();
    			if ( targ->getValue(p) ) {
    				if ( isdigit(*p) ) arg1 = jagatoi(p);
    			} else throw 3445;
			}
			operandStack.pop();
		} else if ( op == JAG_FUNC_LINESUBSTRING ) {
			Jstr endf;
			const char *p = NULL;
			ExprElementNode *t3 = operandStack.top();
			if ( t3->getValue(p) ) {
				endf = p; 
			} else throw 1323;
			operandStack.pop();  // popped the third element

			if ( operandStack.empty() ) throw 1325;
			t3 = operandStack.top();
			Jstr startf;
			if ( t3->getValue(p) ) {
				startf = p; 
			} else throw 1326;
			operandStack.pop();  // popped the startfrac element
			carg1 = startf + ":" + endf;
			// leaving the geom column in stack for right to be assigned
		} else if ( op == JAG_FUNC_POINTN || op == JAG_FUNC_POLYGONN
    					 || op == JAG_FUNC_INTERPOLATE || op == JAG_FUNC_REMOVEPOINT
    		             || op == JAG_FUNC_RINGN || op == JAG_FUNC_INNERRINGN ) {
	    	if ( operandStackTopSize() < 2 ) throw 2425;
    		const char *p = NULL;
        	right = operandStack.top();
    		if ( right->getValue(p) ) {
    			carg1 = p;
    		} else throw 1448;
    		operandStack.pop();
			right = NULL;
    	} else if ( op == JAG_FUNC_TOPOLYGON ) {
	    	if ( 1 == operandStackTopSize() ) {
    			carg1 = "30";
    		} else {
    			const char *p = NULL;
        		right = operandStack.top();
    			if ( right->getValue(p) ) {
    				carg1 = p;
    			} else throw 2932;
    			operandStack.pop();
				right = NULL;
    		}
    	} else if ( op == JAG_FUNC_BUFFER ) {
	    	if ( 1 == operandStackTopSize() ) {
    			carg1 = "distance=symmetric:10,side=side,join=round:10,end=round:10,point=circle:10";
    		} else {
    			const char *p = NULL;
        		right = operandStack.top();
    			if ( right->getValue(p) ) {
    				carg1 = p;
    			} else throw 2935;
    			operandStack.pop();
				right = NULL;
    		}
		}  else if ( op == JAG_FUNC_SCALE || op == JAG_FUNC_SCALESIZE ) {
			int nargs = operandStackTopSize();
			if (  nargs >= 2 ) {
				t3 = operandStack.top();
				if ( t3->getValue(p) ) { carg1 = p; } else throw 1332;
				operandStack.pop();  // popped the third element
			} 

			if ( nargs >= 3 ) {
				t3 = operandStack.top();
				if ( t3->getValue(p) ) { carg1 = Jstr(p) + ":" + carg1; } else throw 1350;
				operandStack.pop();  // popped the third element
			} 
			
			if ( nargs >= 4 ) {
				t3 = operandStack.top();
				if ( t3->getValue(p) ) { carg1 = Jstr(p) + ":" + carg1; } else throw 1351;
				operandStack.pop();  // popped the third element
			}
		}  else if ( op == JAG_FUNC_TRANSLATE ) {
			// translate(geom, dx, dy)
			// translate(geom, dx, dy, dz)
			int nargs = operandStackTopSize();
			t3 = operandStack.top();
			if ( t3->getValue(p) ) { carg1 = p; } else throw 2332;
			operandStack.pop();  // popped the third element
			if ( operandStack.empty() ) throw 4546;

			t3 = operandStack.top();
			if ( t3->getValue(p) ) { carg1 = Jstr(p) + ":" + carg1; } else throw 2350;
			operandStack.pop();  // popped the third element
			
			if ( nargs >= 4 ) {
				t3 = operandStack.top();
				if ( t3->getValue(p) ) { carg1 = Jstr(p) + ":" + carg1; } else throw 2351;
				operandStack.pop();  // popped the third element
			}
		}  else if ( op == JAG_FUNC_TRANSSCALE ) {
			// transscale(geom, dx, dy, xfac, yfac)
			// translate(geom, dx, dy, dz, xfac, yfac, zfac )
			int nargs = operandStackTopSize();
			prt(("s3663 nargs=%d\n", nargs ));
			if ( 5 == nargs ) {
    			t3 = operandStack.top();
    			if ( t3->getValue(p) ) { carg1 = p; } else throw 2452;
    			operandStack.pop();  // popped yfac

    			if ( operandStack.empty() ) throw 4546;
    			t3 = operandStack.top();
    			if ( t3->getValue(p) ) { carg1 = Jstr(p) + ":" + carg1; } else throw 2452;
    			operandStack.pop();  // popped xfac

    			if ( operandStack.empty() ) throw 4547;
    			t3 = operandStack.top();
    			if ( t3->getValue(p) ) { carg1 = Jstr(p) + ":" + carg1; } else throw 2453;
    			operandStack.pop();  // popped dy

    			if ( operandStack.empty() ) throw 4548;
    			t3 = operandStack.top();
    			if ( t3->getValue(p) ) { carg1 = Jstr(p) + ":" + carg1; } else throw 2454;
    			operandStack.pop();  // popped dx

			} else if ( 7 == nargs ) {
				t3 = operandStack.top();
				if ( t3->getValue(p) ) { carg1 = Jstr(p); } else throw 2455;
				operandStack.pop();  // popped zfac

    			if ( operandStack.empty() ) throw 4549;
    			t3 = operandStack.top();
    			if ( t3->getValue(p) ) { carg1 = Jstr(p) + ":" + carg1; } else throw 2456;
    			operandStack.pop();  // popped yfac

    			if ( operandStack.empty() ) throw 4550;
    			t3 = operandStack.top();
    			if ( t3->getValue(p) ) { carg1 = Jstr(p) + ":" + carg1; } else throw 2457;
    			operandStack.pop();  // popped xfac

    			if ( operandStack.empty() ) throw 4551;
    			t3 = operandStack.top();
    			if ( t3->getValue(p) ) { carg1 = Jstr(p) + ":" + carg1; } else throw 2458;
    			operandStack.pop();  // popped dz

    			if ( operandStack.empty() ) throw 4552;
    			t3 = operandStack.top();
    			if ( t3->getValue(p) ) { carg1 = Jstr(p) + ":" + carg1; } else throw 2459;
    			operandStack.pop();  // popped dy

    			if ( operandStack.empty() ) throw 4553;
    			t3 = operandStack.top();
    			if ( t3->getValue(p) ) { carg1 = Jstr(p) + ":" + carg1; } else throw 2460;
    			operandStack.pop();  // popped dx
			} else {
				throw 2582;
			}
		}  else if ( op == JAG_FUNC_ROTATE || op == JAG_FUNC_ROTATESELF ) {
			//prt(("s2938 JAG_FUNC_ROTATE nargs=%d\n", operandStack.size() ));
			//prt(("s2938 JAG_FUNC_ROTATE topsize=%d\n", operandStackTopSize() ));
			// 2D only
			// rotate(geom, 10 )  -- defualt is in degrees, counter-clock-wise
			// rotate(geom, 10, 'degree')
			// rotate(geom, 10, 'radian')
			// rotateself(geom, 10, 'radian')
			t3 = operandStack.top();
			if ( ! t3->getValue(p) ) throw 4601;
			carg1 = p;
			operandStack.pop();  // popped the third element
			if ( ! isdigit(*p) && *p != '.' ) {
				if ( *p == 'd' || *p == 'D' || *p == 'r' || *p == 'R' ) {
					t3 = operandStack.top();
					if ( ! t3->getValue(p) ) throw 4601;
					carg1 = Jstr(p) + ":" + carg1;  // "10.2:degree"
					operandStack.pop();
				} else throw 2340;
			} else {
				// got number in dgrees
				carg1 += ":degree"; 
			}
		}  else if ( op == JAG_FUNC_ROTATEAT ) {
			// 2D only
			// rotateat(geom, 10, 'degree', x0,y0)
			t3 = operandStack.top();
			if ( ! t3->getValue(p) ) throw 4601;
			carg1 = p;
			operandStack.pop();  // popped y0

			if ( operandStack.empty() ) throw 4602;
			t3 = operandStack.top();
			if ( ! t3->getValue(p) ) throw 4603;
			carg1 = Jstr(p) + ":" + carg1;  // "10.2:23.1"
			operandStack.pop();  // popped x0

			if ( operandStack.empty() ) throw 4604;
			t3 = operandStack.top();
			if ( ! t3->getValue(p) ) throw 4605;
			carg1 = Jstr(p) + ":" + carg1;  // "degree:10.2:23.1"
			operandStack.pop();  // popped 'degree'

			if ( operandStack.empty() ) throw 4606;
			t3 = operandStack.top();
			if ( ! t3->getValue(p) ) throw 4607;
			carg1 = Jstr(p) + ":" + carg1;  // "12.3:degree:10.2:23.1"
			operandStack.pop();  // popped angle
		}  else if ( op == JAG_FUNC_AFFINE ) {
			// qwer
			// 2D affine(geom, a, b, d, e, xoff, yoff) -- 6 args
			// 3D Affine(geom, a, b, c, d, e, f, g, h, i, xoff, yoff, zoff)  -- 12 args
			int nargs = operandStackTopSize();
			if ( nargs >= 6 ) {
				// 2D affine
    			t3 = operandStack.top();
    			if ( ! t3->getValue(p) ) throw 4611;
    			carg1 = p;
    			operandStack.pop(); 
    
    			if ( operandStack.empty() ) throw 4612;
    			t3 = operandStack.top();
    			if ( ! t3->getValue(p) ) throw 4613;
    			carg1 = Jstr(p) + ":" + carg1;  
    			operandStack.pop(); 
    
    			if ( operandStack.empty() ) throw 4614;
    			t3 = operandStack.top();
    			if ( ! t3->getValue(p) ) throw 4615;
    			carg1 = Jstr(p) + ":" + carg1;  
    			operandStack.pop();
    
    			if ( operandStack.empty() ) throw 4616;
    			t3 = operandStack.top();
    			if ( ! t3->getValue(p) ) throw 4617;
    			carg1 = Jstr(p) + ":" + carg1;  
    			operandStack.pop(); 

    			if ( operandStack.empty() ) throw 4618;
    			t3 = operandStack.top();
    			if ( ! t3->getValue(p) ) throw 4617;
    			carg1 = Jstr(p) + ":" + carg1;  
    			operandStack.pop(); 

    			if ( operandStack.empty() ) throw 4618;
    			t3 = operandStack.top();
    			if ( ! t3->getValue(p) ) throw 4617;
    			carg1 = Jstr(p) + ":" + carg1;  
    			operandStack.pop(); 
			} else throw 3939;
			if ( nargs >= 12 ) {
    			if ( operandStack.empty() ) throw 4622;
    			t3 = operandStack.top();
    			if ( ! t3->getValue(p) ) throw 4623;
    			carg1 = Jstr(p) + ":" + carg1;  
    			operandStack.pop();  
    
    			if ( operandStack.empty() ) throw 4624;
    			t3 = operandStack.top();
    			if ( ! t3->getValue(p) ) throw 4625;
    			carg1 = Jstr(p) + ":" + carg1;  
    			operandStack.pop(); 
    
    			if ( operandStack.empty() ) throw 4626;
    			t3 = operandStack.top();
    			if ( ! t3->getValue(p) ) throw 4627;
    			carg1 = Jstr(p) + ":" + carg1;  
    			operandStack.pop();

    			if ( operandStack.empty() ) throw 4628;
    			t3 = operandStack.top();
    			if ( ! t3->getValue(p) ) throw 4627;
    			carg1 = Jstr(p) + ":" + carg1;  
    			operandStack.pop();

    			if ( operandStack.empty() ) throw 4638;
    			t3 = operandStack.top();
    			if ( ! t3->getValue(p) ) throw 4637;
    			carg1 = Jstr(p) + ":" + carg1;  
    			operandStack.pop(); 

    			if ( operandStack.empty() ) throw 4648;
    			t3 = operandStack.top();
    			if ( ! t3->getValue(p) ) throw 4647;
    			carg1 = Jstr(p) + ":" + carg1;  
    			operandStack.pop(); 
			}
		} else {
		}

		// left child
		if ( operandStack.empty() ) throw 4446;

		left = operandStack.top();
		operandStack.pop();
	}

	// debug
	//prt(("s4084 new BinaryOpNode with left, right, and push to operandStack op=[%s]\n", opstr.c_str() ));
    //opstr = BinaryOpNode::binaryOpStr(operatorStack.top());
	//prt(("s4094 operatorStack.top()=[%d] [%s]\n", operatorStack.top(),  opstr.c_str() ));

	BinaryOpNode *pn = new BinaryOpNode(this, operatorStack.top(), left, right, _jpa, arg1, arg2, carg1 );
	//operandStack.print();	
	operandStack.push(pn);	
	operandStack.resetTopArgs();	
	//operandStack.print();	
}

short BinaryExpressionBuilder::precedence( short fop )
{
	enum { lowest, low, lowmid, mid, midhigh, high, highest };
	switch (fop) {
		case JAG_LOGIC_OR:  		// or
			return low;
		case JAG_LOGIC_AND: 		// and
			return lowmid;
		case JAG_FUNC_EQUAL:		// = ==
			return mid;
		case JAG_FUNC_NOTEQUAL:		// != <> ><
			return mid;	
		case JAG_FUNC_LESSTHAN:		// <
			return mid;
		case JAG_FUNC_LESSEQUAL:	// <=
			return mid;
		case JAG_FUNC_GREATERTHAN:	// >
			return mid;
		case JAG_FUNC_GREATEREQUAL:	// >=
			return mid;
		case JAG_FUNC_LIKE:			// %
			return mid;
		case JAG_FUNC_MATCH:		// regex
			return mid;
		case JAG_NUM_ADD:			// +
			return midhigh;
		case JAG_NUM_SUB:			// -
			return midhigh;
		case JAG_NUM_MULT:			// *
			return high;
		case JAG_NUM_DIV:			// /
			return high;
		case JAG_NUM_REM:			// %
			return high;
		case JAG_NUM_POW:			// ^
			return highest;
		default:
			return lowest;
	}
}

//  select curdate() 
bool BinaryExpressionBuilder::funcHasZeroChildren( short fop )
{
	if ( fop == JAG_FUNC_CURDATE || fop == JAG_FUNC_CURTIME  || fop == JAG_FUNC_PI
		 || fop == JAG_FUNC_NOW || fop == JAG_FUNC_TIME ) return true;
	return false;
}

// only one constant needed
// select miletometer(12.3) 
bool BinaryExpressionBuilder::funcHasOneConstant( short fop )
{
	if ( fop == JAG_FUNC_TOSECOND || fop == JAG_FUNC_TOMICROSECOND
         || fop == JAG_FUNC_MILETOMETER ) {
		 return true;
	}
	return false;
}

// func(col1, col2, [...])
bool BinaryExpressionBuilder::funcHasTwoChildren( short fop )
{
	if ( fop == JAG_FUNC_MOD || fop == JAG_FUNC_POW 
		 || fop == JAG_FUNC_DATEDIFF 
		 || fop == JAG_LOGIC_AND || fop == JAG_LOGIC_OR || BinaryOpNode::isMathOp(fop) 
		 || fop == JAG_FUNC_WITHIN || fop == JAG_FUNC_COVEREDBY || fop == JAG_FUNC_COVER
		 || fop == JAG_FUNC_STRDIFF || fop == JAG_FUNC_CONTAIN || fop == JAG_FUNC_SAME 
		 || fop == JAG_FUNC_INTERSECT || fop == JAG_FUNC_DISJOINT || fop == JAG_FUNC_CLOSESTPOINT 
		 || fop == JAG_FUNC_ANGLE || fop == JAG_FUNC_UNION || fop == JAG_FUNC_LOCATEPOINT 
		 || fop == JAG_FUNC_DIFFERENCE || fop == JAG_FUNC_SYMDIFFERENCE || fop == JAG_FUNC_INTERSECTION 
		 || fop == JAG_FUNC_COLLECT || fop == JAG_FUNC_SCALEAT
		 || fop == JAG_FUNC_ADDPOINT || fop == JAG_FUNC_SETPOINT 
		 || fop == JAG_FUNC_NEARBY || fop == JAG_FUNC_DISTANCE
		 || BinaryOpNode::isCompareOp(fop) ) {
		 	return true;
	}
	return false;
} // end of funcHasTwoChildren()

bool BinaryExpressionBuilder::checkFuncType( short fop )
{
	if (fop == JAG_FUNC_MIN || fop == JAG_FUNC_MAX || fop == JAG_FUNC_AVG || fop == JAG_FUNC_SUM || 
		fop == JAG_FUNC_STDDEV || fop == JAG_FUNC_FIRST || fop == JAG_FUNC_LAST || fop == JAG_FUNC_ABS || 
		fop == JAG_FUNC_ACOS || fop == JAG_FUNC_ASIN || fop == JAG_FUNC_ATAN || fop == JAG_FUNC_CEIL ||
		fop == JAG_FUNC_COS || fop == JAG_FUNC_COT || fop == JAG_FUNC_FLOOR || fop == JAG_FUNC_LOG2 ||
		fop == JAG_FUNC_LOG10 || fop == JAG_FUNC_LOG || fop == JAG_FUNC_MOD || fop == JAG_FUNC_POW ||
		fop == JAG_FUNC_SIN || fop == JAG_FUNC_SQRT || fop == JAG_FUNC_TAN || fop == JAG_FUNC_SUBSTR ||
		fop == JAG_FUNC_UPPER || fop == JAG_FUNC_LOWER || fop == JAG_FUNC_LTRIM || fop == JAG_FUNC_RTRIM ||
		fop == JAG_FUNC_TRIM || fop == JAG_FUNC_SECOND || fop == JAG_FUNC_MINUTE || fop == JAG_FUNC_HOUR ||
		fop == JAG_FUNC_DATE || fop == JAG_FUNC_MONTH || fop == JAG_FUNC_YEAR || fop == JAG_FUNC_RTRIM ||
		fop == JAG_FUNC_DATEDIFF || fop == JAG_FUNC_DAYOFMONTH || fop == JAG_FUNC_DAYOFWEEK || 
		fop == JAG_FUNC_DAYOFYEAR || fop == JAG_FUNC_CURDATE || fop == JAG_FUNC_CURTIME || fop == JAG_FUNC_NOW || fop == JAG_FUNC_TIME || 
		fop == JAG_FUNC_DISTANCE || fop == JAG_FUNC_WITHIN || 
		fop == JAG_FUNC_COVEREDBY || fop == JAG_FUNC_COVER || fop == JAG_FUNC_CONTAIN || fop == JAG_FUNC_SAME || 
		fop == JAG_FUNC_INTERSECT || fop == JAG_FUNC_DISJOINT || fop == JAG_FUNC_NEARBY || 
		fop == JAG_FUNC_ALL || JAG_FUNC_PI ||
		fop == JAG_FUNC_AREA || fop == JAG_FUNC_CLOSESTPOINT || fop == JAG_FUNC_ANGLE || fop == JAG_FUNC_PERIMETER || 
		fop == JAG_FUNC_VOLUME || fop == JAG_FUNC_DIMENSION || fop == JAG_FUNC_GEOTYPE || fop == JAG_FUNC_POINTN || 
		fop == JAG_FUNC_BBOX || fop == JAG_FUNC_STARTPOINT || fop == JAG_FUNC_ENDPOINT || 
		fop == JAG_FUNC_CONVEXHULL || 
		fop == JAG_FUNC_TOPOLYGON || 
		fop == JAG_FUNC_UNION || fop == JAG_FUNC_DIFFERENCE || fop == JAG_FUNC_SYMDIFFERENCE || 
		fop == JAG_FUNC_INTERSECTION || fop == JAG_FUNC_COLLECT || 
		fop == JAG_FUNC_POLYGONN || fop == JAG_FUNC_OUTERRING || fop == JAG_FUNC_OUTERRINGS || 
		fop == JAG_FUNC_INNERRINGS || fop == JAG_FUNC_RINGN || fop == JAG_FUNC_UNIQUE || 
		fop == JAG_FUNC_INNERRINGN || fop == JAG_FUNC_ASTEXT || fop == JAG_FUNC_BUFFER || 
		fop == JAG_FUNC_CENTROID || 
		fop == JAG_FUNC_ISCLOSED || fop == JAG_FUNC_ISSIMPLE || fop == JAG_FUNC_ISCONVEX || 
		fop == JAG_FUNC_ISVALID || fop == JAG_FUNC_ISRING || 
		fop == JAG_FUNC_ISPOLYGONCCW || fop == JAG_FUNC_ISPOLYGONCW || fop == JAG_FUNC_NUMPOINTS || 
		fop == JAG_FUNC_NUMSEGMENTS || fop == JAG_FUNC_NUMRINGS || fop == JAG_FUNC_NUMPOLYGONS || 
		fop == JAG_FUNC_NUMINNERRINGS || fop == JAG_FUNC_SRID || fop == JAG_FUNC_SUMMARY || 
		fop == JAG_FUNC_XMIN || fop == JAG_FUNC_YMIN || fop == JAG_FUNC_ZMIN || 
		fop == JAG_FUNC_XMAX || fop == JAG_FUNC_YMAX || fop == JAG_FUNC_ZMAX || 
		fop == JAG_FUNC_STRDIFF || fop == JAG_FUNC_TOSECOND || fop == JAG_FUNC_MILETOMETER || 
		fop == JAG_FUNC_TOMICROSECOND || fop == JAG_FUNC_LINELENGTH || fop == JAG_FUNC_INTERPOLATE || 
		fop == JAG_FUNC_LINESUBSTRING || fop == JAG_FUNC_REVERSE || fop == JAG_FUNC_TRANSLATE ||
		fop == JAG_FUNC_TRANSSCALE || fop == JAG_FUNC_AFFINE ||
		fop == JAG_FUNC_ROTATE || fop == JAG_FUNC_ROTATESELF || fop == JAG_FUNC_ROTATEAT ||
		fop == JAG_FUNC_LOCATEPOINT || fop == JAG_FUNC_ADDPOINT || fop == JAG_FUNC_SETPOINT || 
		fop == JAG_FUNC_REMOVEPOINT || fop == JAG_FUNC_SCALE || fop == JAG_FUNC_SCALEAT ||
		fop == JAG_FUNC_DEGREES || fop == JAG_FUNC_RADIANS || fop == JAG_FUNC_SCALESIZE ||
		fop == JAG_FUNC_LENGTH || fop == JAG_FUNC_COUNT ) {
			return true;
	}
	return false;
}  // end checkFuncType()

bool BinaryExpressionBuilder::getCalculationType( const char *p, short &fop, short &len, short &ctype )
{
	// prt(("s0328 BinaryExpressionBuilder::getCalculationType(%s)\n", p ));

	const char *q;
	int rc = 1; fop = 0; len = 0; ctype = 0;

	if ( JAG_NUM_ADD == short(*p) ) {
		fop = JAG_NUM_ADD;
		len = 1; ctype = 0;
	} else if ( JAG_NUM_SUB == short(*p) ) {
		fop = JAG_NUM_SUB;
		len = 1; ctype = 0;
	} else if ( JAG_NUM_MULT == short(*p) ) {
		fop = JAG_NUM_MULT;
		len = 1; ctype = 0;
	} else if ( JAG_NUM_DIV == short(*p) ) {
		fop = JAG_NUM_DIV;
		len = 1; ctype = 0;
	} else if ( JAG_NUM_REM == short(*p) ) {
		fop = JAG_NUM_REM;
		len = 1; ctype = 0;
	} else if ( JAG_NUM_POW == short(*p) ) {
		fop = JAG_NUM_POW;
		len = 1; ctype = 0;
	} else if ( *p == '<' ) {
		if ( *(p+1) == '=' ) {
			fop = JAG_FUNC_LESSEQUAL; len = 2;
		} else if ( *(p+1) == '>' ) {
			fop = JAG_FUNC_NOTEQUAL; len = 2;
		} else {
			fop = JAG_FUNC_LESSTHAN; len = 1;
		}
		ctype = 1;
	} else if ( *p == '>' ) {
		if ( *(p+1) == '=' ) {
			fop = JAG_FUNC_GREATEREQUAL; len = 2;
		} else if ( *(p+1) == '<' ) {
			fop = JAG_FUNC_NOTEQUAL; len = 2;
		} else {
			fop = JAG_FUNC_GREATERTHAN; len = 1;
		}
		ctype = 1;
	} else if ( *p == '!' ) {
		if ( *(p+1) == '=' ) {
			fop = JAG_FUNC_NOTEQUAL; len = 2;
		} else {
			throw 48;
		}
		ctype = 1;
	} else if ( *p == '=' ) {
		if ( *(p+1) == '=' ) {
			len = 2;
		} else {
			len = 1;
		}
		fop = JAG_FUNC_EQUAL; ctype = 1;
	} else if ( 0 == strncasecmp( p, "like ", 5 ) ) {
		fop = JAG_FUNC_LIKE; len = 4; ctype = 1;
	} else if ( 0 == strncasecmp( p, "match ", 6 ) ) {
		fop = JAG_FUNC_MATCH; len = 5; ctype = 1;
	} else if ( 0 == strncasecmp( p, "not ", 4 ) ) {
		// special case: not may affect with "between ... and ..." or " in( "
		len = 3; ctype = 3;
	} else if ( 0 == strncasecmp(p, "max", 3 ) ) {
		fop = JAG_FUNC_MAX; len = 3; ctype = 2;
	} else if ( 0 == strncasecmp(p, "avg", 3 ) ) {
		fop = JAG_FUNC_AVG; len = 3; ctype = 2;
	} else if ( 0 == strncasecmp(p, "summary", 7 ) ) {
		fop = JAG_FUNC_SUMMARY; len = 7; ctype = 2;
	} else if ( 0 == strncasecmp(p, "sum", 3 ) ) {
		fop = JAG_FUNC_SUM; len = 3; ctype = 2;
	} else if ( 0 == strncasecmp(p, "count", 5 ) ) {
		fop = JAG_FUNC_COUNT; len = 5; ctype = 2;
	} else if ( 0 == strncasecmp(p, "stddev", 6 ) ) {
		fop = JAG_FUNC_STDDEV; len = 6; ctype = 2;
	} else if ( 0 == strncasecmp(p, "first", 5 ) ) {
		fop = JAG_FUNC_FIRST; len = 5; ctype = 2;
	} else if ( 0 == strncasecmp(p, "last", 4 ) ) {
		fop = JAG_FUNC_LAST; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "abs", 3 ) ) {
		fop = JAG_FUNC_ABS; len = 3; ctype = 2;
	} else if ( 0 == strncasecmp(p, "acos", 4 ) ) {
		fop = JAG_FUNC_ACOS; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "asin", 4 ) ) {
		fop = JAG_FUNC_ASIN; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "atan", 4 ) ) {
		fop = JAG_FUNC_ATAN; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "ceil", 4 ) ) {
		fop = JAG_FUNC_CEIL; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "cos", 3 ) ) {
		fop = JAG_FUNC_COS; len = 3; ctype = 2;
	} else if ( 0 == strncasecmp(p, "strdiff", 7 ) ) {
		fop = JAG_FUNC_STRDIFF; len = 7; ctype = 2;
	} else if ( 0 == strncasecmp(p, "cot", 3 ) ) {
		fop = JAG_FUNC_COT; len = 3; ctype = 2;
	} else if ( 0 == strncasecmp(p, "floor", 5 ) ) {
		fop = JAG_FUNC_FLOOR; len = 5; ctype = 2;
	} else if ( 0 == strncasecmp(p, "log2", 4 ) ) {
		fop = JAG_FUNC_LOG2; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "log10", 5 ) ) {
		fop = JAG_FUNC_LOG10; len = 5; ctype = 2;
	} else if ( 0 == strncasecmp(p, "log", 3 ) ) {
		fop = JAG_FUNC_LOG; len = 3; ctype = 2;
	} else if ( 0 == strncasecmp(p, "ln", 2 ) ) {
		fop = JAG_FUNC_LOG; len = 2; ctype = 2;
	} else if ( 0 == strncasecmp(p, "mod", 3 ) ) {
		fop = JAG_FUNC_MOD; len = 3; ctype = 2;
	} else if ( 0 == strncasecmp(p, "pow", 3 ) ) {
		fop = JAG_FUNC_POW; len = 3; ctype = 2;
	} else if ( 0 == strncasecmp(p, "radians", 7 ) ) {
		fop = JAG_FUNC_RADIANS; len = 7; ctype = 2;
	} else if ( 0 == strncasecmp(p, "degrees", 7 ) ) {
		fop = JAG_FUNC_DEGREES; len = 7; ctype = 2;
	} else if ( 0 == strncasecmp(p, "sin", 3 ) ) {
		fop = JAG_FUNC_SIN; len = 3; ctype = 2;
	} else if ( 0 == strncasecmp(p, "sqrt", 4 ) ) {
		fop = JAG_FUNC_SQRT; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "tan", 3 ) ) {
		fop = JAG_FUNC_TAN; len = 3; ctype = 2;
	} else if ( 0 == strncasecmp(p, "substring", 9 ) ) {
		fop = JAG_FUNC_SUBSTR; len = 9; ctype = 2;
		if ( _substrClause >= 0 || _datediffClause >= 0 ) throw 9006;
		_substrClause = 0;
	} else if ( 0 == strncasecmp(p, "substr", 6 ) ) {
		fop = JAG_FUNC_SUBSTR; len = 6; ctype = 2;
		if ( _substrClause >= 0 || _datediffClause >= 0 ) throw 9007;
		_substrClause = 0;
	} else if ( 0 == strncasecmp(p, "upper", 5 ) ) {
		fop = JAG_FUNC_UPPER; len = 5; ctype = 2;
	} else if ( 0 == strncasecmp(p, "lower", 5 ) ) {
		fop = JAG_FUNC_LOWER; len = 5; ctype = 2;
	} else if ( 0 == strncasecmp(p, "ltrim", 5 ) ) {
		fop = JAG_FUNC_LTRIM; len = 5; ctype = 2;
	} else if ( 0 == strncasecmp(p, "rtrim", 5 ) ) {
		fop = JAG_FUNC_RTRIM; len = 5; ctype = 2;
	} else if ( 0 == strncasecmp(p, "trim", 4 ) ) {
		fop = JAG_FUNC_TRIM; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "length", 6 ) ) {
		fop = JAG_FUNC_LENGTH; len = 6; ctype = 2;
	} else if ( 0 == strncasecmp(p, "linelength", 10 ) ) {
		fop = JAG_FUNC_LINELENGTH; len = 10; ctype = 2;
	} else if ( 0 == strncasecmp(p, "second", 6 ) ) {
		fop = JAG_FUNC_SECOND; len = 6; ctype = 2;
	} else if ( 0 == strncasecmp(p, "minute", 6 ) ) {
		fop = JAG_FUNC_MINUTE; len = 6; ctype = 2;
	} else if ( 0 == strncasecmp(p, "min", 3 ) ) {
		fop = JAG_FUNC_MIN; len = 3; ctype = 2;
	} else if ( 0 == strncasecmp(p, "hour", 4 ) ) {
		fop = JAG_FUNC_HOUR; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "datediff", 8 ) ) {
		fop = JAG_FUNC_DATEDIFF; len = 8; ctype = 2;
		if ( _substrClause >= 0 || _datediffClause >= 0 ) throw 95;
		_datediffClause = 0;
	} else if ( 0 == strncasecmp(p, "date", 4 ) ) {
		fop = JAG_FUNC_DATE; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "month", 5 ) ) {
		fop = JAG_FUNC_MONTH; len = 5; ctype = 2;
	} else if ( 0 == strncasecmp(p, "year", 4 ) ) {
		fop = JAG_FUNC_YEAR; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "dayofmonth", 10 ) ) {
		fop = JAG_FUNC_DAYOFMONTH; len = 10; ctype = 2;
	} else if ( 0 == strncasecmp(p, "dayofweek", 9 ) ) {
		fop = JAG_FUNC_DAYOFWEEK; len = 9; ctype = 2;
	} else if ( 0 == strncasecmp(p, "dayofyear", 9 ) ) {
		fop = JAG_FUNC_DAYOFYEAR; len = 9; ctype = 2;
	} else if ( 0 == strncasecmp(p, "curdate", 7 ) ) {
		fop = JAG_FUNC_CURDATE; len = 7; ctype = 2;
	} else if ( 0 == strncasecmp(p, "curtime", 7 ) ) {
		fop = JAG_FUNC_CURTIME; len = 7; ctype = 2;
	} else if ( 0 == strncasecmp(p, "now", 3 ) ) {
		fop = JAG_FUNC_NOW; len = 3; ctype = 2;
	} else if ( 0 == strncasecmp(p, "time", 4 ) ) {
		fop = JAG_FUNC_TIME; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "sysdate", 7 ) ) {
		fop = JAG_FUNC_NOW; len = 7; ctype = 2;
	} else if ( 0 == strncasecmp(p, "distance", 8 ) ) {
		fop = JAG_FUNC_DISTANCE; len = 8; ctype = 2;
	} else if ( 0 == strncasecmp(p, "within", 6 ) ) {
		fop = JAG_FUNC_WITHIN; len = 6; ctype = 2;
	} else if ( 0 == strncasecmp(p, "coveredby", 9 ) ) {
		fop = JAG_FUNC_COVEREDBY; len = 9; ctype = 2;
	} else if ( 0 == strncasecmp(p, "cover", 5 ) ) {  // must be after coveredby
		fop = JAG_FUNC_COVER; len = 5; ctype = 2;
	} else if ( 0 == strncasecmp(p, "contain", 7 ) ) {
		fop = JAG_FUNC_CONTAIN; len = 7; ctype = 2;
	} else if ( 0 == strncasecmp(p, "same", 4 ) ) {
		fop = JAG_FUNC_SAME; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "equal", 5 ) ) {
		fop = JAG_FUNC_SAME; len = 5; ctype = 2;
	} else if ( 0 == strncasecmp(p, "intersection", 12 ) ) {
		// must be before the intersect
		fop = JAG_FUNC_INTERSECTION; len = 12; ctype = 2;
	} else if ( 0 == strncasecmp(p, "intersect", 9 ) ) {
		fop = JAG_FUNC_INTERSECT; len = 9; ctype = 2;
	} else if ( 0 == strncasecmp(p, "closestpoint", 12 ) ) {
		fop = JAG_FUNC_CLOSESTPOINT; len = 12; ctype = 2;
	} else if ( 0 == strncasecmp(p, "disjoint", 8 ) ) {
		fop = JAG_FUNC_DISJOINT; len = 8; ctype = 2;
	} else if ( 0 == strncasecmp(p, "nearby", 6 ) ) {
		fop = JAG_FUNC_NEARBY; len = 6; ctype = 2;
	} else if ( 0 == strncasecmp(p, "all", 3 ) ) {
		fop = JAG_FUNC_ALL; len = 3; ctype = 2;
	} else if ( 0 == strncasecmp(p, "tosecond", 8 ) ) {
		fop = JAG_FUNC_TOSECOND; len = 8; ctype = 2;
	} else if ( 0 == strncasecmp(p, "tomicrosecond", 13 ) ) {
		fop = JAG_FUNC_TOMICROSECOND; len = 13; ctype = 2;
	} else if ( 0 == strncasecmp(p, "miletometer", 11 ) ) {
		fop = JAG_FUNC_MILETOMETER; len = 11; ctype = 2;
	} else if ( 0 == strncasecmp(p, "area", 4 ) ) {
		fop = JAG_FUNC_AREA; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "perimeter", 9 ) ) {
		fop = JAG_FUNC_PERIMETER; len = 9; ctype = 2;
	} else if ( 0 == strncasecmp(p, "angle", 5 ) ) {
		fop = JAG_FUNC_ANGLE; len = 5; ctype = 2;
	} else if ( 0 == strncasecmp(p, "volume", 6 ) ) {
		fop = JAG_FUNC_VOLUME; len = 6; ctype = 2;
	} else if ( 0 == strncasecmp(p, "dimension", 9 ) ) {
		fop = JAG_FUNC_DIMENSION; len = 9; ctype = 2;
	} else if ( 0 == strncasecmp(p, "geotype", 7 ) ) {
		fop = JAG_FUNC_GEOTYPE; len = 7; ctype = 2;
	} else if ( 0 == strncasecmp(p, "pointn", 6 ) ) {
		fop = JAG_FUNC_POINTN; len = 6; ctype = 2;
	} else if ( 0 == strncasecmp(p, "startpoint", 10 ) ) {
		fop = JAG_FUNC_STARTPOINT; len = 10; ctype = 2;
	} else if ( 0 == strncasecmp(p, "endpoint", 8 ) ) {
		fop = JAG_FUNC_ENDPOINT; len = 8; ctype = 2;
	} else if ( 0 == strncasecmp(p, "convexhull", 10 ) ) {
		fop = JAG_FUNC_CONVEXHULL; len = 10; ctype = 2;
	} else if ( 0 == strncasecmp(p, "topolygon", 9 ) ) {
		fop = JAG_FUNC_TOPOLYGON; len = 9; ctype = 2;
	} else if ( 0 == strncasecmp(p, "union", 5 ) ) {
		fop = JAG_FUNC_UNION; len = 5; ctype = 2;
	} else if ( 0 == strncasecmp(p, "difference", 10 ) ) {
		fop = JAG_FUNC_DIFFERENCE; len = 10; ctype = 2;
	} else if ( 0 == strncasecmp(p, "symdifference", 13 ) ) {
		fop = JAG_FUNC_SYMDIFFERENCE; len = 13; ctype = 2;
	} else if ( 0 == strncasecmp(p, "collect", 7 ) ) {
		fop = JAG_FUNC_COLLECT; len = 7; ctype = 2;
	} else if ( 0 == strncasecmp(p, "polygonn", 8 ) ) {
		fop = JAG_FUNC_POLYGONN; len = 8; ctype = 2;
	} else if ( 0 == strncasecmp(p, "innerrings", 10 ) ) {
		fop = JAG_FUNC_INNERRINGS; len = 10; ctype = 2;
	} else if ( 0 == strncasecmp(p, "outerrings", 10 ) ) {
		fop = JAG_FUNC_OUTERRINGS; len = 10; ctype = 2;
	} else if ( 0 == strncasecmp(p, "outerring", 9 ) ) {
		fop = JAG_FUNC_OUTERRING; len = 9; ctype = 2;
	} else if ( 0 == strncasecmp(p, "ringn", 5 ) ) {
		fop = JAG_FUNC_RINGN; len = 5; ctype = 2;
	} else if ( 0 == strncasecmp(p, "interpolate", 11 ) ) {
		fop = JAG_FUNC_INTERPOLATE; len = 11; ctype = 2;
	} else if ( 0 == strncasecmp(p, "linesubstring", 13 ) ) {
		fop = JAG_FUNC_LINESUBSTRING; len = 13; ctype = 2;
	} else if ( 0 == strncasecmp(p, "locatepoint", 11 ) ) {
		fop = JAG_FUNC_LOCATEPOINT; len = 11; ctype = 2;
	} else if ( 0 == strncasecmp(p, "astext", 6 ) ) {
		fop = JAG_FUNC_ASTEXT; len = 6; ctype = 2;
	} else if ( 0 == strncasecmp(p, "text", 4 ) ) {
		fop = JAG_FUNC_ASTEXT; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "unique", 6 ) ) {
		fop = JAG_FUNC_UNIQUE; len = 6; ctype = 2;
	} else if ( 0 == strncasecmp(p, "innerringn", 10 ) ) {
		fop = JAG_FUNC_INNERRINGN; len = 10; ctype = 2;
	} else if ( 0 == strncasecmp(p, "buffer", 6 ) ) {
		fop = JAG_FUNC_BUFFER; len = 6; ctype = 2;
	} else if ( 0 == strncasecmp(p, "centroid", 8 ) ) {
		fop = JAG_FUNC_CENTROID; len = 8; ctype = 2;
	} else if ( 0 == strncasecmp(p, "bbox", 4 ) ) {
		fop = JAG_FUNC_BBOX; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "envelope", 8 ) ) {
		fop = JAG_FUNC_BBOX; len = 8; ctype = 2;
	} else if ( 0 == strncasecmp(p, "isclosed", 8 ) ) {
		fop = JAG_FUNC_ISCLOSED; len = 8; ctype = 2;
	} else if ( 0 == strncasecmp(p, "issimple", 8 ) ) {
		fop = JAG_FUNC_ISSIMPLE; len = 8; ctype = 2;
	} else if ( 0 == strncasecmp(p, "isvalid", 7 ) ) {
		fop = JAG_FUNC_ISVALID; len = 7; ctype = 2;
	} else if ( 0 == strncasecmp(p, "isring", 6 ) ) {
		fop = JAG_FUNC_ISRING; len = 6; ctype = 2;
	} else if ( 0 == strncasecmp(p, "ispolygonccw", 12 ) ) {
		fop = JAG_FUNC_ISPOLYGONCCW; len = 12; ctype = 2;
	} else if ( 0 == strncasecmp(p, "ispolygoncw", 11 ) ) {
		fop = JAG_FUNC_ISPOLYGONCW; len = 11; ctype = 2;
	} else if ( 0 == strncasecmp(p, "isconvex", 8 ) ) {
		fop = JAG_FUNC_ISCONVEX; len = 8; ctype = 2;
	} else if ( 0 == strncasecmp(p, "numpoints", 9 ) ) {
		fop = JAG_FUNC_NUMPOINTS; len = 9; ctype = 2;
	} else if ( 0 == strncasecmp(p, "numsegments", 11 ) ) {
		fop = JAG_FUNC_NUMSEGMENTS; len = 11; ctype = 2;
	} else if ( 0 == strncasecmp(p, "numrings", 8 ) ) {
		fop = JAG_FUNC_NUMRINGS; len = 8; ctype = 2;
	} else if ( 0 == strncasecmp(p, "numpolygons", 11 ) ) {
		fop = JAG_FUNC_NUMPOLYGONS; len = 11; ctype = 2;
	} else if ( 0 == strncasecmp(p, "numinnerrings", 13 ) ) {
		fop = JAG_FUNC_NUMINNERRINGS; len = 13; ctype = 2;
	} else if ( 0 == strncasecmp(p, "srid", 4 ) ) {
		fop = JAG_FUNC_SRID; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "xmin", 4 ) ) {
		fop = JAG_FUNC_XMIN; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "ymin", 4 ) ) {
		fop = JAG_FUNC_YMIN; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "zmin", 4 ) ) {
		fop = JAG_FUNC_ZMIN; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "xmax", 4 ) ) {
		fop = JAG_FUNC_XMAX; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "ymax", 4 ) ) {
		fop = JAG_FUNC_YMAX; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "zmax", 4 ) ) {
		fop = JAG_FUNC_ZMAX; len = 4; ctype = 2;
	} else if ( 0 == strncasecmp(p, "addpoint", 8 ) ) {
		fop = JAG_FUNC_ADDPOINT; len = 8; ctype = 2;
	} else if ( 0 == strncasecmp(p, "setpoint", 8 ) ) {
		fop = JAG_FUNC_SETPOINT; len = 8; ctype = 2;
	} else if ( 0 == strncasecmp(p, "removepoint", 11 ) ) {
		fop = JAG_FUNC_REMOVEPOINT; len = 11; ctype = 2;
	} else if ( 0 == strncasecmp(p, "reverse", 7 ) ) {
		fop = JAG_FUNC_REVERSE; len = 7; ctype = 2;
	} else if ( 0 == strncasecmp(p, "scaleat", 7 ) ) {
		fop = JAG_FUNC_SCALEAT; len = 7; ctype = 2;
	} else if ( 0 == strncasecmp(p, "scalesize", 9 ) ) {
		fop = JAG_FUNC_SCALESIZE; len = 9; ctype = 2;
	} else if ( 0 == strncasecmp(p, "scale", 5 ) ) {
		fop = JAG_FUNC_SCALE; len = 5; ctype = 2;
	} else if ( 0 == strncasecmp(p, "translate", 9 ) ) {
		fop = JAG_FUNC_TRANSLATE; len = 9; ctype = 2;
	} else if ( 0 == strncasecmp(p, "transscale", 10 ) ) {
		fop = JAG_FUNC_TRANSSCALE; len = 10; ctype = 2;
	} else if ( 0 == strncasecmp(p, "rotateself", 10 ) ) {
		fop = JAG_FUNC_ROTATESELF; len = 10; ctype = 2;
	} else if ( 0 == strncasecmp(p, "rotateat", 8 ) ) {
		fop = JAG_FUNC_ROTATEAT; len = 8; ctype = 2;
	} else if ( 0 == strncasecmp(p, "rotate", 6 ) ) {
		fop = JAG_FUNC_ROTATE; len = 6; ctype = 2;
	} else if ( 0 == strncasecmp(p, "pi", 2 ) ) {
		fop = JAG_FUNC_PI; len = 2; ctype = 2;
	} else if ( 0 == strncasecmp(p, "affine", 6 ) ) {
		fop = JAG_FUNC_AFFINE; len = 6; ctype = 2;
	} else {
		// ...more functions to be added
		rc = 0;
	}

	// prt(("s3403 fop=%d fopstr=[%s]\n", fop,  BinaryOpNode::binaryOpStr(fop).c_str() ));
	
	// for funcs, need to check if those are colname or func name 
	// ( e.g. find '(' after name and possible spaces )
	if ( 2 == ctype ) {
		q = p+len;
		while ( isspace(*q) ) ++q;
		if ( *q != '(' ) {
			rc = 0;
			if ( JAG_FUNC_SUBSTR == fop ) _substrClause = -1;
			else if ( JAG_FUNC_DATEDIFF == fop ) _datediffClause = -1;
		} else len = q-p+1;
	}
	
	if ( _substrClause >= 0 || _datediffClause >= 0 ) {
		if ( rc != 0 && fop != JAG_FUNC_SUBSTR && fop != JAG_FUNC_DATEDIFF ) throw 94;
	}

	if ( 0 == _datediffClause && fop != JAG_FUNC_DATEDIFF ) {
		// for datediff, first column must be type of: year, month, day, hour, minute, second
		if ( JAG_FUNC_YEAR != fop && JAG_FUNC_MONTH != fop && JAG_FUNC_HOUR != fop &&
			JAG_FUNC_MINUTE != fop && JAG_FUNC_SECOND != fop && 0 != strncasecmp(p, "day", 3 ) ) {
			throw 3293;
		}
	}

	return rc;
}

// name should be one of:
// colname, tab.colname, idx.colname, db.tab.colname, db.idx.colname, db.tab.idx.colname
bool BinaryExpressionBuilder::nameConvertionCheck( Jstr &name, int &tabnum,
													const JagHashMap<AbaxString, AbaxPair<AbaxString, abaxint>> &cmap, 
													Jstr &colList )
{
	JagStrSplit sp( name, '.' );
	AbaxPair<AbaxString, abaxint> pair;	
	if ( !cmap.getValue( "0", pair ) ) return 0; // no totnum, invalid
	Jstr fpath = pair.key.c_str();
	abaxint totnum = pair.value;
	if ( sp.length() == 1 && totnum > 1 ) return 0; // no tab/idx name given for multiple tabs/idxs
	// only one table/index
	if ( 1 == totnum ) {
		if ( sp.length() == 1 ) {
			name = fpath +  "." + sp[0];
		} else if ( sp.length() == 2 ) {
			name = fpath +  "." + sp[1];
		} else {
			name = fpath +  "." + sp[2];
		}
		tabnum = 0;
		if ( colList.length() < 1 ) colList = name;
		else colList += Jstr("|") + name;
		return 1;
	}

	// else, multiple tables/indexs, possible for join
	// get db.tab part from name, exclude column part
	fpath = sp[0];
	for ( int i = 1; i < sp.length()-1; ++i ) {
		fpath += Jstr(".") + sp[i];
	}

	if ( !cmap.getValue( fpath, pair ) ) return 0;

	fpath = pair.key.c_str();
	name = fpath + "." + sp[sp.length()-1]; // db.tab.col
	tabnum = pair.value;
	if ( colList.length() < 1 ) colList = name;
	else colList += Jstr("|") + name;
	return 1;
}

//check if db.tab.col matches with previous one as in "db.tab.col = db.tab.col2"
bool BinaryExpressionBuilder::nameAndOpGood( const JagParser *jpsr, const Jstr &fullname, 
											 const StringElementNode &lastNode )
{
	if ( ! jpsr ) return true;

	Jstr colType;
	JagStrSplit sp(fullname, '.' );
	if ( sp.length() == 3 ) {
		// bool isIndex = jpsr->isIndexCol( sp[0], sp[2] );
		const JagColumn* jcol = jpsr->getColumn( sp[0], sp[1], sp[2] );
		if ( ! jcol ) return true;
		colType = jcol->type;
	}

	// prt(("s3388 _lastOp=[%d] \n", _lastOp ));
	if ( colType.size() > 0 && lastNode._name.size() > 0 && BinaryOpNode::isCompareOp(_lastOp) ) {
		JagStrSplit sp1( lastNode._name, '.' );
		if ( sp1.length() == 3 ) {
			const JagColumn* prevJcol = jpsr->getColumn( sp1[JAG_SP_START+0], sp1[JAG_SP_START+1], sp1[JAG_SP_START+2] );
			if ( ! prevJcol ) return true;

			if ( colType != prevJcol->type ) {
				prt(("s4081 preColType=[%s] != colType=[%s]\n", prevJcol->type.c_str(), colType.c_str() ));
				return false;
			}
		}
	}

	return true;
}

int BinaryExpressionBuilder::operandStackTopSize() const
{
	return operandStack.getTopArgs();
	/***
	ExprElementNode *t;
	int cnt = 0;
	//prt(("s2737 operandStackTopSize size=%d\n", operandStack.size() ));
	for ( int i = operandStack.size()-1; i >=0; --i ) {
		t = operandStack[i];
		//prt(("s2837 i=%d t->getBinaryOp()=%d\n", i, t->getBinaryOp() ));
		if ( t->getBinaryOp() != 0  ) {
			// operator element
			//return cnt;
			break;
		}
		++cnt;
	}

	prt(("s2663 computedcnt=%d  savedtopargs=%d\n", cnt, operandStack.getTopArgs() ));
	return cnt;
	****/
}

// return 0 for OK;  < 0 for error or false
// lstr must be table/index column with its all internal data
// rstr must be table/index column with all internal data or a const string "2 3 4"
// op can be WITHIN, CONTAIN, COVER, COVERED, OVERLAP, INTERSECT
// within: point-> line, linestring, triangle, square, cube, ...
// within:  triangle -> triangle, square, cube, ...
bool BinaryOpNode::processBooleanOp( int op, const AbaxFixString &inlstr, const AbaxFixString &inrstr, 
											const Jstr &carg )
{
	prt(("s5481 do processBooleanOp inlstr=[%s]\n", inlstr.c_str() ));
	prt(("s5481 do processBooleanOp inrstr=[%s]\n", inrstr.c_str() ));
	//prt(("s5481 do processBooleanOp carg=[%s]\n", carg.c_str() ));
	//  lstr : OJAG=srid=name=type=subtype  data1 data2 data3 ...
	//  rstr : OJAG=srid=name=type=subtype  data1 data2 data3 ...
	//  rstr : CJAG=0=0=type=subtype  data1 data2 data3 ...

	if ( inlstr.size() < 1 ) return false;

	Jstr lstr;
	if ( !strnchr( inlstr.c_str(), '=', 8 ) ) {
		int rc1 = JagGeo::convertConstantObjToJAG( inlstr, lstr );
		if ( rc1 <= 0 ) return false;
	} else {
		lstr = inlstr.c_str();
	}

    Jstr rstr;
    if ( !strnchr( inrstr.c_str(), '=', 8 ) ) {
        int rc2 = JagGeo::convertConstantObjToJAG( inrstr, rstr );
        if ( rc2 <= 0 ) return false;
    } else {
        rstr = inrstr.c_str();
    }

	Jstr colobjstr1 = lstr.firstToken(' ');
	Jstr colobjstr2 = rstr.firstToken(' ');
	JagStrSplit spcol2(colobjstr2, '=');  
	Jstr colType2;

	// colobjstr1: "OJAG=srid=db.obj.col=type"
	Jstr colType1;
	JagStrSplit spcol1(colobjstr1, '=');  // OJAG=srid=name=type
	int srid1 = 0;
	Jstr mark1, colName1;  // colname: "db.tab.col"
	if ( spcol1.length() < 4 ) {
		if ( spcol2.length() >= 4 ) {
			colType2 = spcol2[3];
			if ( colType2 == JAG_C_COL_TYPE_RANGE ) {
				if (  _left && _left->_isElement ) {
					mark1 = "OJAG";
					srid1 = _left->_srid;
					colName1 = _left->_name;
					colType1 = _left->_type;
					//prt(("s8273 left is element node. srid1=%d name=[%s] type=[%s]\n", srid1, colName1.c_str(), colType1.c_str() ));
				} else {
					//prt(("s7283 left is not element, use dummy" ));
					mark1 = "OJAG";
					srid1 = 0;
					colName1 = "dummy";
					colType1 = "dummy";
				}
			} else {
				prt(("E4405 not enough header [%s] op=%d\n", lstr.c_str(), op ));
				return false;
			}
		} else {
			//prt(("E4406 not enough header [%s] op=%d\n", lstr.c_str(), op ));
			//return false;
			mark1 = "OJAG";
			srid1 = 0;
			colName1 = "dummy";
			colType1 = "dummy";
		}
	}

	if ( spcol1.length() >= 4 ) {
		mark1 = spcol1[0];
		srid1 = jagatoi( spcol1[1].c_str() );
		colName1 = spcol1[2];
		colType1 = spcol1[3];
	}

	// colobjstr2: "OJAG=srid=db.obj.col=type=subtype 33 44"
	// colobjstr2: "CJAG=0=0=type=subtype bbx 3 4"
	int srid2 = 0;
	Jstr mark2, colName2;  // colname: "db.tab.col"

	if ( spcol2.length() < 4 ) {
		prt(("E4416 not enough header [%s]\n", colobjstr2.c_str() ));
		return false;
	}

	mark2 = spcol2[0];
	srid2 = jagatoi( spcol2[1].c_str() );
	colName2 = spcol2[2];
	colType2 = spcol2[3];

	if ( mark2 == "OJAG" && mark1  == "OJAG" && srid1 != srid2 ) {
		prt(("E4418 two columns but sriddiff hdr1=[%s] hdr2=[%s]\n", colobjstr1.c_str(), colobjstr2.c_str() ));
		return false;
	}

	if ( mark2 == JAG_OJAG && mark1 == JAG_CJAG ) {
		srid1 = srid2;
	} else if (  mark2 == JAG_CJAG && mark1 == JAG_OJAG ) {
		srid2 = srid1;
	}

	JagStrSplit sp1( lstr.c_str(), ' ', true );
	JagStrSplit sp2( rstr.c_str(), ' ', true );

	//sp1.shift();	
	//sp2.shift();	

	bool rc = doBooleanOp( op, mark1, colType1, srid1, sp1, mark2, colType2, srid2, sp2, carg );
	//prt(("s4480 doBooleanOp rc=%d\n", rc ));
	return rc;
}


// return result value 
// lstr must be table/index column with its all internal data
// rstr must be table/index column with all internal data 
// op can be UNION
Jstr  BinaryOpNode::processTwoStrOp( int op, const AbaxFixString &inlstr, const AbaxFixString &inrstr, const Jstr &carg )
{
	prt(("s5481 do processTwoStrOp inlstr=[%s]\n", inlstr.c_str() ));
	prt(("s5481 do processTwoStrOp inrstr=[%s]\n", inrstr.c_str() ));
	//prt(("s5481 do processTwoStrOp carg=[%s]\n", carg.c_str() ));
	//  lstr : OJAG=srid=name=type=subtype bbx  data1 data2 data3 ...
	//  rstr : OJAG=srid=name=type=subtype bbx  data1 data2 data3 ...
	//  rstr : CJAG=0=0=type=subtype bbx  data1 data2 data3 ...
	if ( inlstr.size() < 1 ) return "";
	Jstr lstr;
	if ( !strnchr( inlstr.c_str(), '=', 8 ) ) {
		int rc1 = JagGeo::convertConstantObjToJAG( inlstr, lstr );
		if ( rc1 <= 0 ) {
			prt(("s2019 error convertConstantObjToJAG inlstr rc1=%d\n", rc1 ));
			return "";
		}
	} else {
		lstr = inlstr.c_str();
	}

    Jstr rstr;
    if ( !strnchr( inrstr.c_str(), '=', 8 ) ) {
        int rc2 = JagGeo::convertConstantObjToJAG( inrstr, rstr );
        if ( rc2 <= 0 ) {
			prt(("s2029 error convertConstantObjToJAG inrstr rc2=%d\n", rc2 ));
			return "";
		}
    } else {
        rstr = inrstr.c_str();
    }

	prt(("s5482 do processTwoStrOp lstr=[%s]\n", lstr.c_str() ));
	prt(("s5482 do processTwoStrOp rstr=[%s]\n", rstr.c_str() ));
	Jstr colobjstr1 = lstr.firstToken(' ');
	Jstr colobjstr2 = rstr.firstToken(' ');
	JagStrSplit spcol2(colobjstr2, '=');  
	Jstr colType2;

	// colobjstr1: "OJAG=srid=db.obj.col=type=subtype"
	Jstr colType1;
	JagStrSplit spcol1(colobjstr1, '=');  // OJAG=srid=name=type=subtype
	int srid1 = 0;
	Jstr mark1, colName1;  // colname: "db.tab.col"
	if ( spcol1.length() < 4 ) {
		if ( spcol2.length() >= 4 ) {
			colType2 = spcol2[3];
			if ( colType2 == JAG_C_COL_TYPE_RANGE ) {
				if (  _left && _left->_isElement ) {
					mark1 = "OJAG";
					srid1 = _left->_srid;
					colName1 = _left->_name;
					colType1 = _left->_type;
					//prt(("s8273 left is element node. srid1=%d name=[%s] type=[%s]\n", srid1, colName1.c_str(), colType1.c_str() ));
				} else {
					//prt(("s7283 left is not element, use dummy" ));
					mark1 = "OJAG";
					srid1 = 0;
					colName1 = "dummy";
					colType1 = "dummy";
				}
			} else {
				prt(("E5405 not enough header [%s] op=%d\n", lstr.c_str(), op ));
				return false;
			}
		} else {
			//prt(("E4406 not enough header [%s] op=%d\n", lstr.c_str(), op ));
			//return false;
			mark1 = "OJAG";
			srid1 = 0;
			colName1 = "dummy";
			colType1 = "dummy";
		}
	}

	if ( spcol1.length() >= 4 ) {
		mark1 = spcol1[0];
		srid1 = jagatoi( spcol1[1].c_str() );
		colName1 = spcol1[2];
		colType1 = spcol1[3];
	}

	// colobjstr2: "OJAG=srid=db.obj.col=type=subtype bbx 33 44"
	// colobjstr2: "CJAG=0=0=type=subtype bbx 3 4"
	int srid2 = 0;
	Jstr mark2, colName2;  // colname: "db.tab.col"

	if ( spcol2.length() < 4 ) {
		prt(("E5416 not enough header [%s]\n", colobjstr2.c_str() ));
		return "";
	}

	mark2 = spcol2[0];
	srid2 = jagatoi( spcol2[1].c_str() );
	colName2 = spcol2[2];
	colType2 = spcol2[3];

	if ( mark2 == "OJAG" && mark1  == "OJAG" && srid1 != srid2 ) {
		prt(("E5418 two columns but sriddiff hdr1=[%s] hdr2=[%s]\n", colobjstr1.c_str(), colobjstr2.c_str() ));
		return "";
	}

	if ( mark2 == JAG_OJAG && mark1 == JAG_CJAG ) {
		srid1 = srid2;
	} else if (  mark2 == JAG_CJAG && mark1 == JAG_OJAG ) {
		srid2 = srid1;
	}

	JagStrSplit sp1( lstr.c_str(), ' ', true );
	JagStrSplit sp2( rstr.c_str(), ' ', true );

	//sp1.shift();	
	//sp2.shift();	
	//prt(("s1021 sp1.print sp2.print \n" ));
	//sp1.print();
	//sp2.print();

	return doTwoStrOp( op, mark1, colType1, srid1, sp1, mark2, colType2, srid2, sp2, carg );
}

// return 0 for OK;  < 0 for error or false
// lstr must be table/index column with its all internal data
// op can be AEA
bool BinaryOpNode::processSingleStrOp( int op, const AbaxFixString &inlstr, const Jstr &carg, Jstr &value )
{
	prt(("s5481 do processSingleStrOp inlstr=[%s]\n", inlstr.c_str() ));
	prt(("s5481 do processSingleStrOp carg=[%s]\n", carg.c_str() ));
	//  lstr : OJAG=srid=name=type=subtype  data1 data2 data3 ...
	if ( inlstr.size() < 1 ) return false;
	Jstr lstr;
	if ( !strnchr( inlstr.c_str(), '=', 8 ) ) {
		//prt(("s1390 has no = sign in beginning\n" ));
		int rc1 = JagGeo::convertConstantObjToJAG( inlstr, lstr );
		if ( rc1 <= 0 ) return false;
	} else {
		//prt(("s1029 has = sign\n" ));
		lstr = inlstr.c_str();
	}
	prt(("s5482 do processSingleStrOp lstr=[%s]\n", lstr.c_str() ));

	Jstr colobjstr1 = lstr.firstToken(' ');
	// colobjstr1: "OJAG=srid=db.obj.col=type"
	Jstr colType1;
	JagStrSplit spcol1(colobjstr1, '=');  // OJAG=srid=name=type
	int srid1 = 0;
	Jstr mark1, colName1;  // colname: "db.tab.col"

	if ( spcol1.length() >= 4 ) {
		mark1 = spcol1[0];
		srid1 = jagatoi( spcol1[1].c_str() );
		colName1 = spcol1[2];
		colType1 = spcol1[3];
	}

	JagStrSplit sp1( lstr.c_str(), ' ', true );
	Jstr hdr = sp1[0];
	//sp1.shift();	

	bool rc = doSingleStrOp( op, mark1, hdr, colType1, srid1, sp1, carg, value );
	//prt(("s4480 doBooleanOp rc=%d value=%s\n", rc, value.c_str() ));
	return rc;
}


// return 0 for OK;  < 0 for error or false
// lstr must be table/index column with its all internal data
// op can be AEA
bool BinaryOpNode::processSingleDoubleOp( int op, const AbaxFixString &inlstr, const Jstr &carg, double &value )
{
	prt(("s5481 do processSingleDoubleOp lstr=[%s]\n", inlstr.c_str() ));
	//prt(("s5481 do processSingleDoubleOp carg=[%s]\n", carg.c_str() ));
	//  lstr : OJAG=srid=name=type=subtype  data1 data2 data3 ...
	if ( inlstr.size() < 1 ) return false;
	Jstr lstr;
	if ( !strnchr( inlstr.c_str(), '=', 8 ) ) {
		int rc1 = JagGeo::convertConstantObjToJAG( inlstr, lstr );
		if ( rc1 <= 0 ) return false;
	} else {
		lstr = inlstr.c_str();
	}

	Jstr colobjstr1 = lstr.firstToken(' ');
	// colobjstr1: "OJAG=srid=db.obj.col=type"
	Jstr colType1;
	JagStrSplit spcol1(colobjstr1, '=');  // OJAG=srid=name=type
	int srid1 = 0;
	Jstr mark1, colName1;  // colname: "db.tab.col"

	if ( spcol1.length() >= 4 ) {
		mark1 = spcol1[0];
		srid1 = jagatoi( spcol1[1].c_str() );
		colName1 = spcol1[2];
		colType1 = spcol1[3];
	}

	JagStrSplit sp1( lstr.c_str(), ' ', true );
	//sp1.shift();	

	bool rc = doSingleDoubleOp( op, mark1, colType1, srid1, sp1, carg, value );
	//prt(("s4480 doBooleanOp rc=%d\n", rc ));
	return rc;
}

bool BinaryOpNode::doSingleDoubleOp( int op, const Jstr& mark1, const Jstr &colType1, int srid1, 
										const JagStrSplit &sp1, const Jstr &carg, double &value )
{
	prt(("s2059 doSingleDoubleOp sp1:\n" ));
	//sp1.print();

	bool rc = false;
	if ( op == JAG_FUNC_AREA ) {
		rc = doAllArea( mark1, colType1, srid1, sp1, value );
	} else if ( op == JAG_FUNC_XMIN || op == JAG_FUNC_YMIN || op == JAG_FUNC_ZMIN
	            || op == JAG_FUNC_XMAX || op == JAG_FUNC_YMAX || op == JAG_FUNC_ZMAX ) {
		rc = doAllMinMax( op, mark1, colType1, sp1, value );
	} else if (  op == JAG_FUNC_VOLUME ) {
		rc = doAllVolume( mark1, colType1, srid1, sp1, value );
	} else if (  op == JAG_FUNC_PERIMETER ) {
		rc = doAllPerimeter( mark1, colType1, srid1, sp1, value );
	} else {
	}
	return rc;
}

bool BinaryOpNode::doSingleStrOp( int op, const Jstr& mark1, const Jstr& hdr, const Jstr &colType1, int srid1, 
										JagStrSplit &sp1, const Jstr &carg, Jstr &value )
{
	prt(("s2232 doSingleStrOp srid1=%d  sp1:\n", srid1 ));
	//sp1.print();

	bool rc = false;
	if ( op == JAG_FUNC_POINTN ) {
		rc = doAllPointN( mark1, colType1, srid1, sp1, carg, value );
	} else if ( op == JAG_FUNC_BBOX ) {
		rc = doAllBBox( mark1, colType1, sp1, value );
	} else if ( op == JAG_FUNC_STARTPOINT ) {
		rc = doAllStartPoint( mark1, colType1, sp1, value );
	} else if ( op == JAG_FUNC_ENDPOINT ) {
		rc = doAllEndPoint( mark1, colType1, sp1, value );
	} else if ( op == JAG_FUNC_ISCLOSED ) {
		rc = doAllIsClosed( mark1, colType1, sp1, value );
	} else if ( op == JAG_FUNC_ISSIMPLE ) {
		rc = doAllIsSimple( mark1, colType1, sp1, value );
	} else if ( op == JAG_FUNC_ISVALID ) {
		rc = doAllIsValid( mark1, colType1, sp1, value );
	} else if ( op == JAG_FUNC_ISRING ) {
		rc = doAllIsRing( mark1, colType1, sp1, value );
	} else if ( op == JAG_FUNC_ISPOLYGONCCW ) {
		rc = doAllIsPolygonCCW( mark1, colType1, sp1, value );
	} else if ( op == JAG_FUNC_ISPOLYGONCW ) {
		rc = doAllIsPolygonCW( mark1, colType1, sp1, value );
	} else if ( op == JAG_FUNC_ISCONVEX ) {
		rc = doAllIsConvex( mark1, colType1, sp1, value );
	} else if ( op == JAG_FUNC_NUMPOINTS ) {
		rc = doAllNumPoints( mark1, colType1, sp1, value );
	} else if ( op == JAG_FUNC_NUMSEGMENTS ) {
		rc = doAllNumSegments( mark1, colType1, sp1, value );
	} else if ( op == JAG_FUNC_NUMRINGS ) {
		rc = doAllNumRings( mark1, colType1, sp1, value );
	} else if ( op == JAG_FUNC_NUMINNERRINGS ) {
		rc = doAllNumInnerRings( mark1, colType1, sp1, value );
	} else if ( op == JAG_FUNC_SRID ) {
		value = intToStr(srid1);
		// prt(("s0293 JAG_FUNC_SRID value=[%s]\n", value.c_str() ));
		rc = true;
	} else if ( op == JAG_FUNC_SUMMARY ) {
		rc = doAllSummary( mark1, colType1, srid1, sp1, value );
	} else if ( op == JAG_FUNC_CONVEXHULL ) {
		rc = doAllConvexHull( mark1, hdr, colType1, srid1, sp1, value );
	} else if ( op == JAG_FUNC_OUTERRING ) {
		rc = doAllOuterRing( mark1, hdr, colType1, srid1, sp1, value );
	} else if ( op == JAG_FUNC_UNIQUE ) {
		rc = doAllUnique( mark1, hdr, colType1, sp1, value );
	} else if ( op == JAG_FUNC_OUTERRINGS ) {
		rc = doAllOuterRings( mark1, hdr, colType1, srid1, sp1, value );
	} else if ( op == JAG_FUNC_INNERRINGS ) {
		rc = doAllInnerRings( mark1, hdr, colType1, srid1, sp1, value );
	} else if ( op == JAG_FUNC_RINGN ) {
		rc = doAllRingN( mark1, hdr, colType1, srid1, sp1, carg, value );
	} else if ( op == JAG_FUNC_INNERRINGN ) {
		rc = doAllInnerRingN( mark1, hdr, colType1, srid1, sp1, carg, value );
	} else if ( op == JAG_FUNC_POLYGONN ) {
		rc = doAllPolygonN( mark1, hdr, colType1, sp1, carg, value );
	} else if ( op == JAG_FUNC_NUMPOLYGONS ) {
		rc = doAllNumPolygons( mark1, hdr, colType1, sp1, value );
	} else if ( op == JAG_FUNC_BUFFER ) {
		rc = doAllBuffer( mark1, hdr, colType1, srid1, sp1, carg, value );
	} else if ( op == JAG_FUNC_CENTROID ) {
		rc = doAllCentroid( mark1, hdr, colType1, srid1, sp1, value );
	} else if ( op == JAG_FUNC_TOPOLYGON ) {
		rc = doAllToPolygon( mark1, hdr, colType1, srid1, sp1, carg, value );
	} else if ( op == JAG_FUNC_ASTEXT ) {
		rc = doAllAsText( mark1, hdr, colType1, srid1, sp1, carg, value );
	} else if ( op == JAG_FUNC_INTERPOLATE ) {
		rc = doAllInterpolate( srid1, colType1, sp1, carg, value );
	} else if ( op == JAG_FUNC_LINESUBSTRING ) {
		rc = doAllLineSubstring( srid1, colType1, sp1, carg, value );
	} else if ( op == JAG_FUNC_REMOVEPOINT ) {
		rc = doAllRemovePoint( srid1, colType1, sp1, carg, value );
	} else if ( op == JAG_FUNC_REVERSE ) {
		rc = doAllReverse( srid1, colType1, sp1, carg, value );
	} else if ( op == JAG_FUNC_SCALE ) {
		rc = doAllScale( srid1, colType1, sp1, carg, value );
	} else if ( op == JAG_FUNC_SCALESIZE ) {
		rc = doAllScaleSize( srid1, colType1, sp1, carg, value );
	} else if ( op == JAG_FUNC_TRANSLATE ) {
		rc = doAllTranslate( srid1, colType1, sp1, carg, value );
	} else if ( op == JAG_FUNC_TRANSSCALE ) {
		rc = doAllTransScale( srid1, colType1, sp1, carg, value );
	} else if ( op == JAG_FUNC_ROTATE || op == JAG_FUNC_ROTATEAT ) {
		rc = doAllRotateAt( srid1, colType1, sp1, carg, value );
	} else if ( op == JAG_FUNC_ROTATESELF ) {
		rc = doAllRotateSelf( srid1, colType1, sp1, carg, value );
	} else if ( op == JAG_FUNC_AFFINE ) {
		rc = doAllAffine( srid1, colType1, sp1, carg, value );
	} else {
	}
	//prt(("s2337 rc=%d\n", rc ));
	return rc;
}

bool BinaryOpNode::doBooleanOp( int op, const Jstr& mark1, const Jstr &colType1, int srid1, 
										const JagStrSplit &sp1, const Jstr& mark2, 
										const Jstr &colType2, int srid2, const JagStrSplit &sp2, 
										const Jstr &carg )
{
	bool rc = false;
	if ( srid1 != srid2 ) {
		prt(("s0881 srid1=%d != srid2=%d\n", srid1, srid2 ));
		return rc;
	}

	if ( op == JAG_FUNC_WITHIN ) {
		//prt(("s4350 JAG_FUNC_WITHIN ...\n" ));
		rc = doAllWithin( mark1, colType1, srid1, sp1, mark2, colType2, srid2, sp2, true );
	} else if ( op == JAG_FUNC_COVEREDBY ) {
		//prt(("s4350 JAG_FUNC_COVEREDBY ...\n" ));
		rc = doAllWithin( mark1, colType1, srid1, sp1, mark2, colType2, srid2, sp2, false );
	} else if ( op == JAG_FUNC_CONTAIN ) {
		rc = doAllWithin( mark2, colType2, srid2, sp2, mark1, colType1, srid1, sp1,  true );
	} else if ( op == JAG_FUNC_SAME ) {
		rc = doAllSame( mark1, colType1, srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( op == JAG_FUNC_COVER ) {
		//prt(("s4450 JAG_FUNC_COVER ...\n" ));
		rc = doAllWithin( mark2, colType2, srid2, sp2, mark1, colType1, srid1, sp1,  false );
	} else if ( op == JAG_FUNC_INTERSECT ) {
		rc = doAllIntersect( mark1, colType1, srid1, sp1, mark2, colType2, srid2, sp2, false );
	} else if ( op == JAG_FUNC_DISJOINT ) {
		//rc = ! doAllIntersect( mark2, colType2, srid2, sp2, mark1, colType1, srid1, sp1, false );
		rc = ! doAllIntersect( mark1, colType1, srid1, sp1, mark2, colType2, srid2, sp2, false );
	} else if ( op == JAG_FUNC_NEARBY ) {
		//rc = doAllNearby( mark2, colType2, srid2, sp2, mark1, colType1, srid1, sp1, carg );
		rc = doAllNearby( mark1, colType1, srid1, sp1, mark2, colType2, srid2, sp2, carg );
	} else {
		//prt(("s6023 doBooleanOp op=%d\n", op ));
	}
	//prt(("s1102 doBooleanOp rc=%d\n", rc ));
	return rc;
}

Jstr BinaryOpNode::doTwoStrOp( int op, const Jstr& mark1, const Jstr &colType1, int srid1, 
										JagStrSplit &sp1, const Jstr& mark2, 
										const Jstr &colType2, int srid2, JagStrSplit &sp2, 
										const Jstr &carg )
{
	prt(("s2271 doTwoStrOp colType1=%s colType2=%s \n", colType1.c_str(), colType2.c_str() ));
	//sp1.print();
	//sp2.print();
	if ( srid1 != srid2 ) {
		prt(("s0881 srid1=%d != srid2=%d\n", srid1, srid2 ));
		return "";
	}

	bool rc;
	Jstr res;
	if ( op == JAG_FUNC_CLOSESTPOINT ) {
		if ( colType1 == JAG_C_COL_TYPE_POINT || colType1 == JAG_C_COL_TYPE_POINT3D ) {
			rc = doAllClosestPoint( mark1, colType1, srid1, sp1, mark2, colType2, srid2, sp2, res );
		} else if ( colType2 == JAG_C_COL_TYPE_POINT || colType2 == JAG_C_COL_TYPE_POINT3D ) {
			rc = doAllClosestPoint( mark2, colType2, srid2, sp2, mark1, colType1, srid1, sp1, res );
		} else {
			return "";
		}
		prt(("s0192 JAG_FUNC_CLOSESTPOINT rc=%d\n", rc ));
		if ( ! rc ) return ""; else return res;
	} else if ( op == JAG_FUNC_ANGLE ) {
		if ( colType1 == JAG_C_COL_TYPE_LINE && colType2 == JAG_C_COL_TYPE_LINE ) {
			rc = doAllAngle2D( mark1, colType1, srid1, sp1, mark2, colType2, srid2, sp2, res );
		} else if ( colType1 == JAG_C_COL_TYPE_LINE3D && colType2 == JAG_C_COL_TYPE_LINE3D ) {
			rc = doAllAngle3D( mark1, colType1, srid1, sp1, mark2, colType2, srid2, sp2, res );
		} else {
			return "";
		}
		prt(("s0193 JAG_FUNC_ANGLE rc=%d\n", rc ));
		if ( ! rc ) return ""; else return res;
	} else if ( op == JAG_FUNC_UNION ) {
		return doAllUnion( mark1, colType1, srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( op == JAG_FUNC_COLLECT ) {
		return doAllCollect( mark1, colType1, srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( op == JAG_FUNC_INTERSECTION ) {
		return doAllIntersection( mark1, colType1, sp1, mark2, colType2, sp2 );
	} else if ( op == JAG_FUNC_DIFFERENCE ) {
		return doAllDifference( mark1, colType1, sp1, mark2, colType2, sp2 );
	} else if ( op == JAG_FUNC_SYMDIFFERENCE ) {
		return doAllSymDifference( mark1, colType1, sp1, mark2, colType2, sp2 );
	} else if ( op == JAG_FUNC_LOCATEPOINT ) {
		return doAllLocatePoint( srid1, mark1, colType1, sp1, mark2, colType2, sp2 );
	} else if ( op == JAG_FUNC_ADDPOINT ) {
		return doAllAddPoint( srid1, mark1, colType1, sp1, mark2, colType2, sp2, carg );
	} else if ( op == JAG_FUNC_SETPOINT ) {
		return doAllSetPoint( srid1, mark1, colType1, sp1, mark2, colType2, sp2, carg );
	} else if ( op == JAG_FUNC_SCALEAT ) {
		if ( colType2 == JAG_C_COL_TYPE_POINT || colType2 == JAG_C_COL_TYPE_POINT3D ) {
			return doAllScaleAt( srid1, mark1, colType1, sp1, mark2, colType2, sp2, carg );
		} else {
			return "";
		}
	} else {
		return "";
	}
}

// colType1 and colType2 must be LINE or LINE3D type
bool BinaryOpNode::doAllAngle2D( const Jstr& mark1, const Jstr &colType1, int srid1, 
								 JagStrSplit &sp1, const Jstr& mark2, 
								 const Jstr &colType2, int srid2, JagStrSplit &sp2, Jstr &res )
{
	//prt(("s5387 doAllAngle2D mark1=%s colType1=%s srid1=%d sp1:\n", mark1.c_str(), colType1.c_str(), srid1 ));
	//sp1.print();
	//prt(("s5388 doAllAngle2D mark2=%s colType2=%s srid2=%d sp2:\n", mark2.c_str(), colType2.c_str(), srid2 ));
	//sp2.print();

	double x1, y1, x2, y2;
	x1 = jagatof( sp1[JAG_SP_START+0].c_str() );
	y1 = jagatof( sp1[JAG_SP_START+1].c_str() );
	x2 = jagatof( sp1[JAG_SP_START+2].c_str() );
	y2 = jagatof( sp1[JAG_SP_START+3].c_str() );

	double p1, q1, p2, q2;
	p1 = jagatof( sp2[JAG_SP_START+0].c_str() );
	q1 = jagatof( sp2[JAG_SP_START+1].c_str() );
	p2 = jagatof( sp2[JAG_SP_START+2].c_str() );
	q2 = jagatof( sp2[JAG_SP_START+3].c_str() );

	// cos(theta) = P*Q/(|P|*|Q|}   theta = acos  * 180/PI
	double vx = x2-x1;
	double vy = y2-y1;
	double wx = p2-p1;
	double wy = q2-q1;
	if ( JagGeo::jagEQ(vx, wx) && JagGeo::jagEQ(vy, wy) ) {
		res = "0.0";
		return true;
	}

	double t = ( vx*wx + vy*wy ) / ( sqrt(vx*vx+vy*vy) * sqrt( wx*wx+wy*wy ));
	res = doubleToStr( acos(t) * 180.0/ JAG_PI );
	return true;
}

// colType1 and colType2 must be LINE or LINE3D type
bool BinaryOpNode::doAllAngle3D( const Jstr& mark1, const Jstr &colType1, int srid1, 
								 JagStrSplit &sp1, const Jstr& mark2, 
								 const Jstr &colType2, int srid2, JagStrSplit &sp2, Jstr &res )
{
	prt(("s5887 doAllAngle3D mark1=%s colType1=%s srid1=%d sp1:\n", mark1.c_str(), colType1.c_str(), srid1 ));
	//sp1.print();
	prt(("s5888 doAllAngle3D mark2=%s colType2=%s srid2=%d sp2:\n", mark2.c_str(), colType2.c_str(), srid2 ));
	//sp2.print();

	double x1, y1, z1, x2, y2, z2;
	x1 = jagatof( sp1[JAG_SP_START+0].c_str() );
	y1 = jagatof( sp1[JAG_SP_START+1].c_str() );
	z1 = jagatof( sp1[JAG_SP_START+2].c_str() );
	x2 = jagatof( sp1[JAG_SP_START+3].c_str() );
	y2 = jagatof( sp1[JAG_SP_START+4].c_str() );
	z2 = jagatof( sp1[JAG_SP_START+5].c_str() );

	double p1, q1, r1,  p2, q2, r2;
	p1 = jagatof( sp2[JAG_SP_START+0].c_str() );
	q1 = jagatof( sp2[JAG_SP_START+1].c_str() );
	r1 = jagatof( sp2[JAG_SP_START+2].c_str() );
	p2 = jagatof( sp2[JAG_SP_START+3].c_str() );
	q2 = jagatof( sp2[JAG_SP_START+4].c_str() );
	r2 = jagatof( sp2[JAG_SP_START+5].c_str() );

	// cos(theta) = P*Q/(|P|*|Q|}   theta = acos  * 180/PI
	double vx = x2-x1;
	double vy = y2-y1;
	double vz = z2-z1;
	double wx = p2-p1;
	double wy = q2-q1;
	double wz = r2-r1;
	if ( JagGeo::jagEQ(vx, wx) && JagGeo::jagEQ(vy, wy) && JagGeo::jagEQ(vz, wz) ) {
		res = "0.0";
		return true;
	}
	double t = ( vx*wx + vy*wy + vz*wz ) / ( sqrt(vx*vx+vy*vy+vz*vz) * sqrt(wx*wx+wy*wy+wz*wz));
	res = doubleToStr( acos(t) * 180.0/ JAG_PI );
	return true;
}

// colType1 must be point or point3D type
bool BinaryOpNode::doAllClosestPoint( const Jstr& mark1, const Jstr &colType1, int srid1, 
										const JagStrSplit &sp1, const Jstr& mark2, 
										const Jstr &colType2, int srid2, const JagStrSplit &sp2, Jstr &res )
{
	double px, py, pz;
	if ( mark1 == JAG_OJAG ) {
		if ( colType1 == JAG_C_COL_TYPE_POINT ) {
			/**
				i=0 [OJAG=0=test.mpg.p1=MG=0]
				i=1 [0.0:0.0:8.0:9.0]  // bbox
				i=2 [0.0:0.0]
			**/
			JagStrSplit c( sp1[JAG_SP_START+0], ':' );
			px = jagatof( c[0].c_str() );
			py = jagatof( c[1].c_str() );
		} else {
			JagStrSplit c( sp1[JAG_SP_START+0], ':' );
			px = jagatof( c[0].c_str() );
			py = jagatof( c[1].c_str() );
			pz = jagatof( c[2].c_str() );
		}
	} else {
		/**
			i=0 [CJAG=0=0=PT=0]
			i=1 [0.0:0.0:8.0:9.0]  // bbox
			i=2 [x]
			i=3 [y]
		**/
		px = jagatof( sp1[JAG_SP_START+0].c_str() );
		py = jagatof( sp1[JAG_SP_START+1].c_str() );
		if ( colType1 == JAG_C_COL_TYPE_POINT3D ) {
			pz = jagatof( sp1[JAG_SP_START+3].c_str() );
		} 
	}

	prt(("s5887 doClosestPoint mark1=%s colType1=%s srid1=%d sp1:\n", mark1.c_str(), colType1.c_str(), srid1 ));
	//sp1.print();
	prt(("s5888 doClosestPoint mark2=%s colType2=%s srid2=%d sp2:\n", mark2.c_str(), colType2.c_str(), srid2 ));
	//sp2.print();
	bool rc =  JagGeo::doClosestPoint(colType1, srid1, px, py, pz, mark2, colType2, sp2, res  );
	return rc;
}

bool BinaryOpNode::doAllWithin( const Jstr& mark1, const Jstr &colType1, int srid1, 
										const JagStrSplit &sp1, const Jstr& mark2, 
										const Jstr &colType2, int srid2, const JagStrSplit &sp2, bool strict )
{
	prt(("s2315 colType1=[%s] \n", colType1.c_str() ));

	if ( colType1 == JAG_C_COL_TYPE_POINT ) {
		return JagGeo::doPointWithin(  sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_POINT3D ) {
		return JagGeo::doPoint3DWithin(  sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE ) {
		return JagGeo::doCircleWithin(  srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE3D ) {
		return JagGeo::doCircle3DWithin(  srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_SPHERE ) {
		return JagGeo::doSphereWithin(  srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE ) {
		return JagGeo::doSquareWithin(  srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE3D ) {
		return JagGeo::doSquare3DWithin(  srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_CUBE ) {
		return JagGeo::doCubeWithin(  srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE ) {
		return JagGeo::doRectangleWithin(  srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE3D ) {
		return JagGeo::doRectangle3DWithin(  srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_BOX ) {
		return JagGeo::doBoxWithin(  srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE ) {
		return JagGeo::doTriangleWithin(  srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE3D ) {
		return JagGeo::doTriangle3DWithin(  srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_CYLINDER ) {
		return JagGeo::doCylinderWithin(  srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_CONE ) {
		return JagGeo::doConeWithin(  srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE ) {
		return JagGeo::doEllipseWithin(  srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSOID ) {
		return JagGeo::doEllipsoidWithin(  srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_LINE ) {
		return JagGeo::doLineWithin(  srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_LINE3D ) {
		return JagGeo::doLine3DWithin(  srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING ) {
		return JagGeo::doLineStringWithin(  mark1, srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return JagGeo::doLineString3DWithin(  mark1, srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		return JagGeo::doPolygonWithin(  mark1, srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON3D ) {
		return JagGeo::doPolygon3DWithin(  mark1, srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT ) {
		return JagGeo::doLineStringWithin(  mark1, srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		return JagGeo::doLineString3DWithin(  mark1, srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING ) {
		return JagGeo::doPolygonWithin(  mark1, srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		return JagGeo::doPolygon3DWithin(  mark1, srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		return JagGeo::doMultiPolygonWithin(  mark1, srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		return JagGeo::doMultiPolygon3DWithin(  mark1, srid1, sp1, mark2, colType2,  srid2, sp2, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_RANGE
	             || colType1 == JAG_C_COL_TYPE_DOUBLE 
	             || colType1 == JAG_C_COL_TYPE_FLOAT
	             || colType1 == JAG_C_COL_TYPE_DATETIME
	             || colType1 == JAG_C_COL_TYPE_TIME
	             || colType1 == JAG_C_COL_TYPE_DATE
	             || colType1 == JAG_C_COL_TYPE_DINT
	             || colType1 == JAG_C_COL_TYPE_DBIGINT
	             || colType1 == JAG_C_COL_TYPE_DSMALLINT
	             || colType1 == JAG_C_COL_TYPE_DTINYINT
	             || colType1 == JAG_C_COL_TYPE_DMEDINT
			  ) {
		return JagRange::doRangeWithin(_jpa, mark1, colType1, srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} 

	// prt(("s2411 colType1=[%s] not handled, false\n", colType1.c_str() ));
	return false;
}

bool BinaryOpNode::doAllSame( const Jstr& mark1, const Jstr &colType1, int srid1, 
								const JagStrSplit &sp1, const Jstr& mark2, 
								const Jstr &colType2, int srid2, const JagStrSplit &sp2 )
{
	prt(("s2315 colType1=[%s] \n", colType1.c_str() ));

	if ( colType1 == JAG_C_COL_TYPE_POINT ) {
		return JagGeo::doPointSame(  sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_POINT3D ) {
		return JagGeo::doPoint3DSame(  sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE ) {
		return JagGeo::doCircleSame(  srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE3D ) {
		return JagGeo::doCircle3DSame(  srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_SPHERE ) {
		return JagGeo::doSphereSame(  srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE ) {
		return JagGeo::doSquareSame(  srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE3D ) {
		return JagGeo::doSquare3DSame(  srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_CUBE ) {
		return JagGeo::doCubeSame(  srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE ) {
		return JagGeo::doRectangleSame(  srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE3D ) {
		return JagGeo::doRectangle3DSame(  srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_BOX ) {
		return JagGeo::doBoxSame(  srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE ) {
		return JagGeo::doTriangleSame(  srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE3D ) {
		return JagGeo::doTriangle3DSame(  srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_CYLINDER ) {
		return JagGeo::doCylinderSame(  srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_CONE ) {
		return JagGeo::doConeSame(  srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE ) {
		return JagGeo::doEllipseSame(  srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSOID ) {
		return JagGeo::doEllipsoidSame(  srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_LINE ) {
		return JagGeo::doLineSame(  srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_LINE3D ) {
		return JagGeo::doLine3DSame(  srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING ) {
		return JagGeo::doLineStringSame(  mark1, srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return JagGeo::doLineString3DSame(  mark1, srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		return JagGeo::doPolygonSame(  mark1, srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON3D ) {
		return JagGeo::doPolygon3DSame(  mark1, srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT ) {
		return JagGeo::doLineStringSame(  mark1, srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		return JagGeo::doLineString3DSame(  mark1, srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING ) {
		return JagGeo::doPolygonSame(  mark1, srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		return JagGeo::doPolygon3DSame(  mark1, srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		return JagGeo::doMultiPolygonSame(  mark1, srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		return JagGeo::doMultiPolygon3DSame(  mark1, srid1, sp1, mark2, colType2,  srid2, sp2);
	} else if ( colType1 == JAG_C_COL_TYPE_RANGE ) {
		return JagRange::doRangeSame(_jpa, mark1, colType1, srid1, sp1, mark2, colType2, srid2, sp2);
	} 

	// prt(("s2411 colType1=[%s] not handled, false\n", colType1.c_str() ));
	return false;
}

bool BinaryOpNode::doAllPointN( const Jstr& mk1, const Jstr &colType1, int srid1, 
									 JagStrSplit &sp1, const  Jstr &carg, Jstr &value )
{
	//prt(("s3920 doAllPointN() colType1=[%s] carg=[%s] sp1.print(): \n", colType1.c_str(), carg.c_str() ));
	// sp1.print();
	//prt(("s3039 sp1.length()=%d\n", sp1.length() ));
	int narg = jagatoi( carg.c_str());
	if ( narg < 1 ) return false;
	int i = narg - 1;
	bool rc = false;
	value = "";
	if ( colType1 == JAG_C_COL_TYPE_POINT ) {
		//prt(("s2031 narg=%d sp1.length()=%d\n", narg, sp1.length() ));
		if ( narg == 1 && sp1.length() >= 2 ) {
			value = sp1[JAG_SP_START+0].trim0() + " " + sp1[JAG_SP_START+1].trim0();
			rc =  true;
		} 
	} else if ( colType1 == JAG_C_COL_TYPE_POINT3D ) {
		if ( narg == 1 && sp1.length() >= 3 ) {
			value = sp1[JAG_SP_START+0].trim0() + " " + sp1[JAG_SP_START+1].trim0() + " " + sp1[JAG_SP_START+2].trim0();
			rc  = true;
		} 
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE ) {
		if (  narg >= 1 && narg <= 3 && sp1.length() >= 6 ) {
			value = sp1[JAG_SP_START+2*i].trim0() + " " + sp1[JAG_SP_START+2*i+1].trim0();
			// x0 y0 x1 y1 x2 y2
			rc = true;
		} 
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE3D ) {
		if (  narg >= 1 && narg <= 3 && sp1.length() >= 9 ) {
			value = sp1[JAG_SP_START+3*i].trim0() + " " + sp1[JAG_SP_START+3*i+1].trim0() + " " + sp1[JAG_SP_START+3*i+2].trim0();
			// x0 y0 z0 x1 y1 z1 x2 y2 z2
			rc = true;
		} 
	} else if ( colType1 == JAG_C_COL_TYPE_LINE ) {
		if (  narg >= 1 && narg <= 2 && sp1.length() >= 4 ) {
			value = sp1[JAG_SP_START+2*i].trim0() + " " + sp1[JAG_SP_START+2*i+1].trim0();
			// x0 y0 x1 y1
			rc = true;
		} 
	} else if ( colType1 == JAG_C_COL_TYPE_LINE3D ) {
		if (  narg >= 1 && narg <= 2 && sp1.length() >= 6 ) {
			value = sp1[JAG_SP_START+3*i].trim0() + " " + sp1[JAG_SP_START+3*i+1].trim0() + " " + sp1[JAG_SP_START+3*i+2].trim0();
			// x0 y0 z0 x1 y1 z1
			rc = true;
		} 
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING || colType1 == JAG_C_COL_TYPE_MULTIPOINT ) {
		if (  narg >= 1 && narg <= sp1.length() ) {
			value = sp1[JAG_SP_START+i];
			value.replace( ':', ' ');
			// x0 y0 x1 y1
			rc = true;
		} 
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING3D || colType1 == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		if (  narg >= 1 && narg <= sp1.length() ) {
			value = sp1[JAG_SP_START+i];
			value.replace( ':', ' ');
			// x0 y0 z0  x1 y1 z1  x2 y2 z2
			rc = true;
		} 
	} 

	//prt(("s2411 colType1=[%s] value=[%s]\n",  colType1.c_str(), value.c_str() ));
	return rc;
}

bool BinaryOpNode::doAllBBox( const Jstr& mk, const Jstr &colType, const JagStrSplit &sp, Jstr &value )
{
	//prt(("s3420 doAllBBox() colType=[%s] mk=[%s] sp1.print(): \n", colType.c_str(), mk.c_str()  ));
	//sp.print();
	//prt(("s6340 doAllBBox sp[0]=[%s]\n", sp[0].c_str() ));
	//prt(("s3539 sp1.length()=%d\n", sp1.length() ));
	bool rc = false;
	value = "";
	if ( strchrnum( sp[JAG_SP_START+0].c_str(), ':' ) >= 4 ) {
		value = sp[0];
		value.replace(':', ' ' );
		rc = true;
	} else {
		// CJAG
		/**
		i=0 [CJAG=0=0=LS=0]
		     bbx
		i=1 [0:0]
		i=2 [2:3]
		i=3 [4:5]
		i=4 [9:3]
		i=5 [33:22]
		**/
		Jstr bbox;
		int rc2;
		if ( colType == JAG_C_COL_TYPE_POLYGON ) {
		    //prt(("s8383 sp.c_str()=[%s]\n", sp.c_str() ));
			//char *sec = secondTokenStart( sp.c_str() );
			//if ( ! sec ) return false;
			JagPolygon pgon;
			// rc2 = JagParser::addPolygonData( pgon, sec, true, false );
			rc2 = JagParser::addPolygonData( pgon, sp, true );
			//rc = JagParser::addPolygon3DData( pgon, sec, false, false );
			//rc1 = JagParser::addMultiPolygonData( pgvec, p1, false, false, false );
			if ( rc2 <= 0 ) return false; 
			double xmin,ymin,xmax,ymax;
			pgon.bbox2D( xmin, ymin, xmax, ymax );
			value = doubleToStr(xmin) + " " + doubleToStr(ymin) + " " + doubleToStr(xmax) + " " + doubleToStr(ymax);
		} else if ( colType == JAG_C_COL_TYPE_POLYGON3D ) {
			//char *sec = secondTokenStart( sp.c_str() );
			//if ( ! sec ) return false;
			JagPolygon pgon;
			// rc2 = JagParser::addPolygon3DData( pgon, sec, true, false );
			rc2 = JagParser::addPolygon3DData( pgon, sp, true );
			if ( rc2 <= 0 ) return false;
			double xmin,ymin,zmin,xmax,ymax, zmax;
			pgon.bbox3D( xmin, ymin, zmin, xmax, ymax, zmax );
			value = doubleToStr(xmin) + " " + doubleToStr(ymin) + " " + doubleToStr(zmin) + " "
			        + doubleToStr(xmax) + " " + doubleToStr(ymax) + " " + doubleToStr(zmax);
		} else if ( colType == JAG_C_COL_TYPE_MULTILINESTRING ) {
			//char *sec = secondTokenStart( sp.c_str() );
			//if ( ! sec ) return false;
			JagPolygon pgon;
			// rc2 = JagParser::addPolygonData( pgon, sec, true, false );
			rc2 = JagParser::addPolygonData( pgon, sp, true );
			if ( rc2 <= 0 ) return false;
			double xmin,ymin,xmax,ymax;
			pgon.bbox2D( xmin, ymin, xmax, ymax );
			value = doubleToStr(xmin) + " " + doubleToStr(ymin) + " " + doubleToStr(xmax) + " " + doubleToStr(ymax);
		} else if ( colType == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
			//char *sec = secondTokenStart( sp.c_str() );
			//if ( ! sec ) return false;
			JagPolygon pgon;
			// rc2 = JagParser::addPolygon3DData( pgon, sec, true, false );
			rc2 = JagParser::addPolygon3DData( pgon, sp, true );
			if ( rc2 <= 0 ) return false;
			double xmin,ymin,zmin,xmax,ymax, zmax;
			pgon.bbox3D( xmin, ymin, zmin, xmax, ymax, zmax );
			value = doubleToStr(xmin) + " " + doubleToStr(ymin) + " " + doubleToStr(zmin) + " "
			        + doubleToStr(xmax) + " " + doubleToStr(ymax) + " " + doubleToStr(zmax);
		} else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON ) {
			//char *sec = secondTokenStart( sp.c_str() );
			//if ( ! sec ) return false;
			JagVector<JagPolygon> pgvec;
			// rc2 = JagParser::addMultiPolygonData( pgvec, sec, true, false, false );
			rc2 = JagParser::addMultiPolygonData( pgvec, sp, true, false );
			if ( rc2 <= 0 ) return false;
			double xmin,ymin,xmax,ymax;
			bool rb = JagGeo::getBBox2D( pgvec, xmin,ymin,xmax,ymax );
			if ( rb ) {
				value = doubleToStr(xmin) + " " + doubleToStr(ymin) + " " + doubleToStr(xmax) + " " + doubleToStr(ymax);
			} else {
				return false;
			}
		} else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
			//char *sec = secondTokenStart( sp.c_str() );
			//if ( ! sec ) return false;
			JagVector<JagPolygon> pgvec;
			// rc2 = JagParser::addMultiPolygonData( pgvec, sec, true, false, true );
			rc2 = JagParser::addMultiPolygonData( pgvec, sp, true, true );
			if ( rc2 <= 0 ) return false;
			double xmin,ymin,zmin,xmax,ymax,zmax;
			bool rb = JagGeo::getBBox3D( pgvec, xmin,ymin,zmin, xmax,ymax,zmax );
			if ( rb ) {
				value = doubleToStr(xmin) + " " + doubleToStr(ymin) + " " + doubleToStr(zmin) + " "
			            + doubleToStr(xmax) + " " + doubleToStr(ymax) + " " + doubleToStr(zmax);
			} else {
				return false;
			}
		} else {
			value = JagGeo::bboxstr( sp, false );
		}
		rc = true;

		// static bool getBBox2D( const JagVector<JagPolygon> &pgvec, double &xmin, double &ymin, double &xmax, double &ymax );
	}

	//prt(("s2035 sp[0]=[%s]\n", sp[0].c_str() ));
	//prt(("s2039 sp[1]=[%s]\n", sp[1].c_str() ));
	// prt(("s2411 colType=[%s] value=[%s] rc=%d\n",  colType.c_str(), value.c_str(), rc ));
	return rc;
}

bool BinaryOpNode::doAllStartPoint( const Jstr& mk, const Jstr &colType, const JagStrSplit &sp, Jstr &value )
{
	//prt(("s3420 doAllBBox() colType1=[%s] carg=[%s] sp1.print(): \n", colType1.c_str(), carg.c_str() ));
	value = "";
	if ( JagParser::isVectorGeoType( colType ) 
	     && colType != JAG_C_COL_TYPE_TRIANGLE 
	     && colType != JAG_C_COL_TYPE_TRIANGLE3D 
	   ) {
		 return false;
	 }

	//sp.print();
	//prt(("s3553 sp.length()=%d\n", sp.length() ));

	if ( colType == JAG_C_COL_TYPE_LINE ) {
    	value = trimEndZeros(sp[JAG_SP_START+0]) + " " + trimEndZeros(sp[JAG_SP_START+1]);
	} else if ( colType == JAG_C_COL_TYPE_LINE3D  ) {
    	value = trimEndZeros(sp[JAG_SP_START+0]) + " " + trimEndZeros(sp[JAG_SP_START+1]) + " " + trimEndZeros(sp[JAG_SP_START+2]);
	} else if ( colType == JAG_C_COL_TYPE_TRIANGLE ) {
		value = trimEndZeros(sp[JAG_SP_START+0]) + " " + trimEndZeros(sp[JAG_SP_START+1]);
	} else if ( colType == JAG_C_COL_TYPE_TRIANGLE3D ) {
		value = trimEndZeros(sp[JAG_SP_START+0]) + " " + trimEndZeros(sp[JAG_SP_START+1]) + " " + trimEndZeros(sp[JAG_SP_START+2]);
	} else {
    	JagStrSplit s( sp[JAG_SP_START+0], ':' );
    	if ( s.length() < 4 ) {
    		value = sp[JAG_SP_START+0]; 
    	} else {
    		value = JagGeo::safeGetStr(sp, JAG_SP_START+1);
    	}
    	value.replace(':', ' ' );
	}

	//prt(("s2035 sp[0]=[%s]\n", sp[0].c_str() ));
	//prt(("s2039 sp[1]=[%s]\n", sp[1].c_str() ));
	// prt(("s2411 colType=[%s] value=[%s]\n",  colType.c_str(), value.c_str() ));
	return true;
}

bool BinaryOpNode::doAllCentroid( const Jstr& mk, const Jstr& hdr, const Jstr &colType, 
								    int srid, const JagStrSplit &sp1, Jstr &value )
{
	prt(("s3420 doAllCentroid() mk=[%s] colType=[%s] sp1.print(): \n", mk.c_str(), colType.c_str() ));
	//sp.print();
	value = "";
	double cx, cy, cz;
	bool rc = true;
	bool is3D = false;

		prt(("s8830 JAG_OJAG\n" ));
		//sp.print();
		JagLineString line;
		JagPolygon pgon;
        if ( colType == JAG_C_COL_TYPE_LINESTRING || colType == JAG_C_COL_TYPE_MULTIPOINT ) {
			JagParser::addLineStringData( line, sp1 );
			line.center2D( cx, cy, false );
        } else if ( colType == JAG_C_COL_TYPE_LINESTRING3D || colType == JAG_C_COL_TYPE_MULTIPOINT3D ) {
			JagLineString3D line3D;
			JagParser::addLineString3DData( line3D, sp1 );
			line3D.center3D( cx, cy, cz, false );
			is3D = true;
        } else if ( colType == JAG_C_COL_TYPE_POLYGON ) {
		    JagParser::addPolygonData( pgon, sp1, true );
			line.copyFrom( pgon.linestr[0], true );
			line.center2D( cx, cy, false );
        } else if ( colType == JAG_C_COL_TYPE_MULTILINESTRING ) {
		    JagParser::addPolygonData( pgon, sp1, true );
			line.copyFrom( pgon.linestr[0], false );
			line.center2D( cx, cy, false );
        } else if ( colType == JAG_C_COL_TYPE_POLYGON3D ) {
		    JagParser::addPolygon3DData( pgon, sp1, true );
			line.copyFrom( pgon.linestr[0], true );
			line.center3D( cx, cy, cz, false );
			is3D = true;
        } else if ( colType == JAG_C_COL_TYPE_LINESTRING3D ) {
		    JagParser::addPolygon3DData( pgon, sp1, true );
			line.copyFrom( pgon.linestr[0], false );
			line.center3D( cx, cy, cz, false );
			is3D = true;
        } else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON ) {
			JagVector<JagPolygon> pgvec;
			JagParser::addMultiPolygonData( pgvec, sp1, true, false );
			line.copyFrom( pgvec[0].linestr[0], true );
			line.center2D( cx, cy, false );
        } else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
			//prt(("s5038 JAG_C_COL_TYPE_MULTIPOLYGON3D\n" ));
			JagVector<JagPolygon> pgvec;
			JagParser::addMultiPolygonData( pgvec, sp1, true, true );
			line.copyFrom( pgvec[0].linestr[0], true );
			line.center3D( cx, cy, cz, false );
			is3D = true;
			//prt(("s9993 print: \n" ));
			//line.print();
		} else if ( colType == JAG_C_COL_TYPE_POINT || colType == JAG_C_COL_TYPE_SQUARE
                    || colType == JAG_C_COL_TYPE_RECTANGLE 
					|| JAG_C_COL_TYPE_CIRCLE || JAG_C_COL_TYPE_ELLIPSE ) {
			cx = jagatof( sp1[JAG_SP_START+0].c_str() );
			cy = jagatof( sp1[JAG_SP_START+1].c_str() );
		} else if ( colType == JAG_C_COL_TYPE_LINE ) {
			double cx1 = jagatof( sp1[JAG_SP_START+0].c_str() );
			double cy1 = jagatof( sp1[JAG_SP_START+1].c_str() );
			double cx2 = jagatof( sp1[JAG_SP_START+2].c_str() );
			double cy2 = jagatof( sp1[JAG_SP_START+3].c_str() );
			cx = ( cx1 + cx2 ) /2.0;
			cy = ( cy1 + cy2 ) /2.0;
		} else if ( colType == JAG_C_COL_TYPE_TRIANGLE ) {
			double cx1 = jagatof( sp1[JAG_SP_START+0].c_str() );
			double cy1 = jagatof( sp1[JAG_SP_START+1].c_str() );
			double cx2 = jagatof( sp1[JAG_SP_START+2].c_str() );
			double cy2 = jagatof( sp1[JAG_SP_START+3].c_str() );
			double cx3 = jagatof( sp1[JAG_SP_START+4].c_str() );
			double cy3 = jagatof( sp1[JAG_SP_START+5].c_str() );
			cx = ( cx1 + cx2 + cx3 ) /3.0;
			cy = ( cy1 + cy2 + cy3 ) /3.0;
		} else if ( colType == JAG_C_COL_TYPE_POINT3D || colType == JAG_C_COL_TYPE_CUBE 
				    || colType == JAG_C_COL_TYPE_SPHERE || colType == JAG_C_COL_TYPE_ELLIPSOID
					|| colType == JAG_C_COL_TYPE_CONE ) {
			cx = jagatof( sp1[JAG_SP_START+0].c_str() );
			cy = jagatof( sp1[JAG_SP_START+1].c_str() );
			cz = jagatof( sp1[JAG_SP_START+2].c_str() );
			//prt(("s7942 JAG_C_COL_TYPE_POINT3D cx=%f cy=%f cz=%f\n", cx, cy, cz ));
			is3D = true;
		} else if ( colType == JAG_C_COL_TYPE_LINE3D ) {
			double cx1 = jagatof( sp1[JAG_SP_START+0].c_str() );
			double cy1 = jagatof( sp1[JAG_SP_START+1].c_str() );
			double cz1 = jagatof( sp1[JAG_SP_START+2].c_str() );
			double cx2 = jagatof( sp1[JAG_SP_START+3].c_str() );
			double cy2 = jagatof( sp1[JAG_SP_START+4].c_str() );
			double cz2 = jagatof( sp1[JAG_SP_START+5].c_str() );
			cx = ( cx1 + cx2 ) /2.0;
			cy = ( cy1 + cy2 ) /2.0;
			cz = ( cz1 + cz2 ) /2.0;
			is3D = true;
		} else if ( colType == JAG_C_COL_TYPE_TRIANGLE3D ) {
			double cx1 = jagatof( sp1[JAG_SP_START+0].c_str() );
			double cy1 = jagatof( sp1[JAG_SP_START+1].c_str() );
			double cz1 = jagatof( sp1[JAG_SP_START+2].c_str() );
			double cx2 = jagatof( sp1[JAG_SP_START+3].c_str() );
			double cy2 = jagatof( sp1[JAG_SP_START+4].c_str() );
			double cz2 = jagatof( sp1[JAG_SP_START+5].c_str() );
			double cx3 = jagatof( sp1[JAG_SP_START+6].c_str() );
			double cy3 = jagatof( sp1[JAG_SP_START+7].c_str() );
			double cz3 = jagatof( sp1[JAG_SP_START+8].c_str() );
			cx = ( cx1 + cx2 + cx3 ) /3.0;
			cy = ( cy1 + cy2 + cy3 ) /3.0;
			cz = ( cz1 + cz2 + cz3 ) /3.0;
			is3D = true;
		} else  {
			rc = false;
		}

	if ( rc ) {
		prt(("s5088 is3D=%d cx=%f cy=%f cz=%f\n", is3D, cx, cy, cz ));
		if ( is3D ) {
			value = trimEndZeros( doubleToStr(cx) ) + " " + trimEndZeros( doubleToStr(cy) ) + " " + trimEndZeros(doubleToStr(cz));
		} else {
			value = trimEndZeros( doubleToStr(cx) ) + " " + trimEndZeros( doubleToStr(cy) );
		}
	}

	return rc;
}

bool BinaryOpNode::doAllConvexHull( const Jstr& mk, const Jstr& hdr, const Jstr &colType, 
								    int srid, const JagStrSplit &sp, Jstr &value )
{
	//prt(("s3420 doAllConvexHull() mk=[%s] colType=[%s] sp1.print(): \n", mk.c_str(), colType.c_str() ));
	//sp.print();
	value = "";
	Jstr bbox;
	if ( mk == JAG_OJAG ) { bbox = sp[1]; } 

		//prt(("s8830 JAG_OJAG\n" ));
		//sp.print();
		JagLineString line;
		JagPolygon pgon;
        if ( colType == JAG_C_COL_TYPE_LINESTRING || colType == JAG_C_COL_TYPE_MULTIPOINT ) {
			JagParser::addLineStringData( line, sp );
			JagCGAL::getConvexHull2DStr( line, hdr, bbox, value );
        } else if ( colType == JAG_C_COL_TYPE_LINESTRING3D || colType == JAG_C_COL_TYPE_MULTIPOINT3D ) {
			JagLineString3D line3D;
			JagParser::addLineString3DData( line3D, sp );
			line = line3D;
			JagCGAL::getConvexHull3DStr( line, hdr, bbox, value );
        } else if ( colType == JAG_C_COL_TYPE_POLYGON ) {
		    JagParser::addPolygonData( pgon, sp, true );
			line.copyFrom( pgon.linestr[0], true );
			JagCGAL::getConvexHull2DStr( line, hdr, bbox, value );
        } else if ( colType == JAG_C_COL_TYPE_MULTILINESTRING ) {
		    JagParser::addPolygonData( pgon, sp, false );
			for ( int i=1; i < pgon.size(); ++i ) {
				line.appendFrom( pgon.linestr[i], false );
			}
			JagCGAL::getConvexHull2DStr( line, hdr, bbox, value );
        } else if ( colType == JAG_C_COL_TYPE_POLYGON3D ) {
		    JagParser::addPolygon3DData( pgon, sp, true );
			line.copyFrom( pgon.linestr[0], true );
			JagCGAL::getConvexHull3DStr( line, hdr, bbox, value );
        } else if ( colType == JAG_C_COL_TYPE_LINESTRING3D ) {
		    JagParser::addPolygon3DData( pgon, sp, true );
			line.copyFrom( pgon.linestr[0], false );
			JagCGAL::getConvexHull3DStr( line, hdr, bbox, value );
        } else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON ) {
			JagVector<JagPolygon> pgvec;
			JagParser::addMultiPolygonData( pgvec, sp, false, false );
			for ( int i=0; i < pgvec.size(); ++i ) {
				line.appendFrom( pgvec[i].linestr[0], true );
			}
			JagCGAL::getConvexHull2DStr( line, hdr, bbox, value );
        } else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
			JagVector<JagPolygon> pgvec;
			JagParser::addMultiPolygonData( pgvec, sp, false, true );
			for ( int i=0; i < pgvec.size(); ++i ) {
				line.appendFrom( pgvec[i].linestr[0], true );
			}
			JagCGAL::getConvexHull3DStr( line, hdr, bbox, value );
		} else  {
		}

	return true;
}

bool BinaryOpNode::doAllAsText( const Jstr& mk, const Jstr& hdr, const Jstr &colType, 
								    int srid, const JagStrSplit &sp, const Jstr& carg, Jstr &value )
{
	prt(("s3420 doAllAsText() srid=%d mk=[%s] colType=[%s] sp1.print(): \n", srid, mk.c_str(), colType.c_str() ));
	//sp.print();
	value = sp.c_str();
	return true;
}

bool BinaryOpNode::doAllInterpolate( int srid, const Jstr &colType, const JagStrSplit &sp, const Jstr& carg, Jstr &value )
{
	prt(("s3453 doAllInterpolate() srid=%d colType=[%s] sp1.print(): \n", srid, colType.c_str() ));
	//sp.print();
	value = "";

	double frac = carg.tof();
	//JagLineString line;
	JagLineString3D line;
	JagPolygon pgon;
	bool rc = false;
	JagPoint3D point;

	if ( colType == JAG_C_COL_TYPE_LINE ) {
		value = Jstr("CJAG=0=0=PT=d 0:0:0:0 ");
		double px, py;
		px = jagatof( sp[JAG_SP_START+0] );
		py = jagatof( sp[JAG_SP_START+1] );
		line.add(px, py);
		px = jagatof( sp[JAG_SP_START+2] );
		py = jagatof( sp[JAG_SP_START+3] );
		line.add(px, py);
		rc = line.interpolatePoint( JAG_3D, srid, frac, point );
		prt(("s5029 rc=%d\n", rc ));
		if ( ! rc ) return false;
		value += d2s( point.x ) + " " + d2s( point.y ); 
	} else if ( colType == JAG_C_COL_TYPE_LINE3D ) {
		value = Jstr("CJAG=0=0=PT3=d 0:0:0:0:0:0 ");
		double px, py, pz;
		px = jagatof( sp[JAG_SP_START+0] );
		py = jagatof( sp[JAG_SP_START+1] );
		pz = jagatof( sp[JAG_SP_START+2] );
		line.add(px, py);
		px = jagatof( sp[JAG_SP_START+3] );
		py = jagatof( sp[JAG_SP_START+4] );
		pz = jagatof( sp[JAG_SP_START+5] );
		line.add(px, py);
		rc = line.interpolatePoint( JAG_2D, srid, frac, point );
		if ( ! rc ) return false;
		value += d2s( point.x ) + " " + d2s( point.y )  + " " + d2s(point.z);
    } else if ( colType == JAG_C_COL_TYPE_LINESTRING ) {
		value = Jstr("CJAG=0=0=PT=d 0:0:0:0 ");
	    JagParser::addPolygonData( pgon, sp, true );
		//line.copyFrom( pgon.linestr[0], true );
		//rc = line.interpolatePoint( JAG_2D, srid, frac, point );
		rc = pgon.linestr[0].interpolatePoint( JAG_2D, srid, frac, point );
		if ( ! rc ) return false;
		value += d2s( point.x ) + " " + d2s( point.y ); 
    } else if ( colType == JAG_C_COL_TYPE_LINESTRING3D ) {
		value = Jstr("CJAG=0=0=PT3=d 0:0:0:0:0:0 ");
	    JagParser::addPolygon3DData( pgon, sp, true );
		//line.copyFrom( pgon.linestr[0], true );
		//rc = line.interpolatePoint( JAG_3D, srid, frac, point );
		rc = pgon.linestr[0].interpolatePoint( JAG_3D, srid, frac, point );
		if ( ! rc ) return false;
		value += d2s( point.x ) + " " + d2s( point.y )  + " " + d2s(point.z);
	} else {
		return false;
	}

	return true;
}

bool BinaryOpNode::doAllLineSubstring( int srid, const Jstr &colType, const JagStrSplit &sp, const Jstr& carg, Jstr &value )
{
	prt(("s3453 doAllLineSubstring() srid=%d colType=[%s] sp1.print(): \n", srid, colType.c_str() ));
	//sp.print();
	value = "";

	JagStrSplit fs(carg, ':' );
	if ( fs.size() < 2 ) return false;

	double startfrac = fs[0].tof();
	double endfrac = fs[1].tof();
	if ( startfrac > endfrac ) return false;
	JagLineString3D line;
	JagPolygon pgon;
	bool rc = false;
	Jstr retLstr;

	if ( colType == JAG_C_COL_TYPE_LINE ) {
		JagPoint3D point;
		value = Jstr("CJAG=0=0=LS=d 0:0:0:0 ");
		double px, py;
		px = jagatof( sp[JAG_SP_START+0] );
		py = jagatof( sp[JAG_SP_START+1] );
		line.add(px, py);
		px = jagatof( sp[JAG_SP_START+2] );
		py = jagatof( sp[JAG_SP_START+3] );
		line.add(px, py);
		rc = line.interpolatePoint( JAG_2D, srid, startfrac, point );
		if ( ! rc ) return false;
		value += d2s(point.x) + ":" + d2s(point.y) +  " ";
		rc = line.interpolatePoint( JAG_2D, srid, endfrac, point );
		if ( ! rc ) return false;
		value += d2s(point.x) + ":" + d2s(point.y);
	} else if ( colType == JAG_C_COL_TYPE_LINE3D ) {
		JagPoint3D point;
		value = Jstr("CJAG=0=0=LS3=d 0:0:0:0:0:0 ");
		double px, py, pz;
		px = jagatof( sp[JAG_SP_START+0] );
		py = jagatof( sp[JAG_SP_START+1] );
		pz = jagatof( sp[JAG_SP_START+2] );
		line.add(px, py);
		px = jagatof( sp[JAG_SP_START+3] );
		py = jagatof( sp[JAG_SP_START+4] );
		pz = jagatof( sp[JAG_SP_START+5] );
		line.add(px, py);

		rc = line.interpolatePoint( JAG_3D, srid, startfrac, point );
		if ( ! rc ) return false;
		value += d2s(point.x) + ":" + d2s(point.y) + ":" + d2s(point.z) +  " ";
		rc = line.interpolatePoint( JAG_2D, srid, endfrac, point );
		if ( ! rc ) return false;
		value += d2s(point.x) + ":" + d2s(point.y) + ":" + d2s(point.z);
    } else if ( colType == JAG_C_COL_TYPE_LINESTRING ) {
		value = Jstr("CJAG=0=0=LS=d 0:0:0:0 ");
	    JagParser::addPolygonData( pgon, sp, true );
		rc = pgon.linestr[0].substring( JAG_2D, srid, startfrac, endfrac, retLstr );
		if ( ! rc ) return false;
		value += retLstr;
    } else if ( colType == JAG_C_COL_TYPE_LINESTRING3D ) {
		value = Jstr("CJAG=0=0=LS3=d 0:0:0:0:0:0 ");
	    JagParser::addPolygon3DData( pgon, sp, true );
		rc = pgon.linestr[0].substring( JAG_3D, srid, startfrac, endfrac, retLstr );
		if ( ! rc ) return false;
		value += retLstr;
	} else {
		return false;
	}

	return true;
}

bool BinaryOpNode::doAllToPolygon( const Jstr& mk, const Jstr& hdr, const Jstr &colType, 
								    int srid, const JagStrSplit &sp, const Jstr& carg, Jstr &value )
{
	prt(("s3420 doAllToPolygon() srid=%d mk=[%s] colType=[%s] sp1.print(): \n", srid, mk.c_str(), colType.c_str() ));
	//sp.print();
	if ( ! JagParser::isVectorGeoType( colType ) ) {
		value = sp.c_str();
		return true;
	}

	value = "";

	double px, py;
    if ( colType == JAG_C_COL_TYPE_SQUARE ) {
		JagSquare2D sq( sp, srid );
		JagPolygon pgon( sq );
		pgon.toJAG( false, true, "", srid, value );
    } else if ( colType == JAG_C_COL_TYPE_RECTANGLE ) {
		JagRectangle2D rect( sp, srid );
		JagPolygon pgon( rect );
		pgon.toJAG( false, true, "", srid, value );
    } else if ( colType == JAG_C_COL_TYPE_CIRCLE ) {
		JagCircle2D cir( sp, srid );
		JagPolygon pgon( cir, jagatoi( carg.c_str()) );
		pgon.toJAG( false, true, "", srid, value );
    } else if ( colType == JAG_C_COL_TYPE_ELLIPSE ) {
		JagEllipse2D e( sp, srid );
		JagPolygon pgon( e, jagatoi( carg.c_str()) );
		pgon.toJAG( false, true, "", srid, value );
    } else if ( colType == JAG_C_COL_TYPE_TRIANGLE ) {
		JagTriangle2D t( sp, srid );
		JagPolygon pgon( t );
		pgon.toJAG( false, true, "", srid, value );
	}

	return true;
}

bool BinaryOpNode::doAllOuterRing( const Jstr& mk, const Jstr& hdr, const Jstr &colType, 
								    int srid, const JagStrSplit &sp, Jstr &value )
{
	prt(("s3420 doAllOuterRing() mk=[%s] colType=[%s] sp1.print(): \n", mk.c_str(), colType.c_str() ));
	//sp.print();
	value = "";
	Jstr bbox;
	if ( mk == JAG_OJAG ) { bbox = sp[1]; } 

	JagLineString line;
	JagPolygon pgon;
    if ( colType == JAG_C_COL_TYPE_POLYGON ) {
	    JagParser::addPolygonData( pgon, sp, true );
		line.copyFrom( pgon.linestr[0], true );
		JagCGAL::getRingStr( line, hdr, bbox, false, value );
    } else if ( colType == JAG_C_COL_TYPE_POLYGON3D ) {
	    JagParser::addPolygon3DData( pgon, sp, true );
		line.copyFrom( pgon.linestr[0], true );
		JagCGAL::getRingStr( line, hdr, bbox, true, value );
    } else {
		return false;
	}

	return true;
}

bool BinaryOpNode::doAllOuterRings( const Jstr& mk, const Jstr& hdr, const Jstr &colType, 
								    int srid, const JagStrSplit &sp, Jstr &value )
{
	//prt(("s3420 doAllOuterRings() mk=[%s] colType=[%s] sp1.print(): \n", mk.c_str(), colType.c_str() ));
	//sp.print();
	value = "";
	Jstr bbox;
	if ( mk == JAG_OJAG ) { bbox = sp[1]; } 
	JagVector<JagPolygon> pgvec;
    if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON ) {
	    JagParser::addMultiPolygonData( pgvec, sp, true, false );
		JagCGAL::getOuterRingsStr( pgvec, hdr, bbox, false, value );
    } else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
	    JagParser::addMultiPolygonData( pgvec, sp, true, true );
		JagCGAL::getOuterRingsStr( pgvec, hdr, bbox, true, value );
    } else if ( colType == JAG_C_COL_TYPE_POLYGON ) {
		JagPolygon pgon;
	    JagParser::addPolygonData( pgon, sp, true );
		pgvec.append( pgon );
		JagCGAL::getOuterRingsStr( pgvec, hdr, bbox, false, value );
    } else if ( colType == JAG_C_COL_TYPE_POLYGON3D ) {
		JagPolygon pgon;
	    JagParser::addPolygon3DData( pgon, sp, true );
		pgvec.append( pgon );
		JagCGAL::getOuterRingsStr( pgvec, hdr, bbox, true, value );
    } else {
		return false;
	}

	return true;
}

bool BinaryOpNode::doAllInnerRings( const Jstr& mk, const Jstr& hdr, const Jstr &colType, 
								    int srid, const JagStrSplit &sp, Jstr &value )
{
	prt(("s3420 doAllInnerRings() mk=[%s] colType=[%s] sp1.print(): \n", mk.c_str(), colType.c_str() ));
	//sp.print();
	value = "";
	Jstr bbox;
	if ( mk == JAG_OJAG ) { bbox = sp[1]; } 
	JagVector<JagPolygon> pgvec;
	int rc;
    if ( colType == JAG_C_COL_TYPE_POLYGON ) {
		JagPolygon pgon;
	    rc = JagParser::addPolygonData( pgon, sp, false );
		prt(("s2838 addPolygonData rc=%d pgon.size=%d\n", rc, pgon.size() ));
		pgvec.append( pgon );
		JagCGAL::getInnerRingsStr( pgvec, hdr, bbox, false, value );
    } else if ( colType == JAG_C_COL_TYPE_POLYGON3D ) {
		JagPolygon pgon;
	    rc = JagParser::addPolygon3DData( pgon, sp, false );
		prt(("s2848 addPolygonData rc=%d pgon.size=%d\n", rc, pgon.size() ));
		pgvec.append( pgon );
		JagCGAL::getInnerRingsStr( pgvec, hdr, bbox, true, value );
    } else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON ) {
	    JagParser::addMultiPolygonData( pgvec, sp, false, false );
		JagCGAL::getInnerRingsStr( pgvec, hdr, bbox, false, value );
    } else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
	    JagParser::addMultiPolygonData( pgvec, sp, false, true );
		JagCGAL::getInnerRingsStr( pgvec, hdr, bbox, true, value );
    } else {
		return false;
	}

	return true;
}

bool BinaryOpNode::doAllRingN( const Jstr& mk, const Jstr& hdr, const Jstr &colType, 
								    int srid, const JagStrSplit &sp, const Jstr &carg,  Jstr &value )
{
	prt(("s3420 doAllRingN() mk=[%s] colType=[%s] carg=[%s] sp1.print(): \n", mk.c_str(), colType.c_str(), carg.c_str() ));
	//sp.print();
	int N = jagatoi( carg.c_str() );
	if ( N <= 0 ) {
		return false;
	}
	value = "";
	Jstr bbox;
	if ( mk == JAG_OJAG ) { bbox = sp[1]; } 

	JagLineString line;
	JagPolygon pgon;
    if ( colType == JAG_C_COL_TYPE_POLYGON ) {
	    JagParser::addPolygonData( pgon, sp, false );
		prt(("s1229 pgon.size=%d %d\n", pgon.size(), pgon.linestr.size() ));
		if ( pgon.linestr.size() < N ) return false;
		line.copyFrom( pgon.linestr[N-1], true );
		JagCGAL::getRingStr( line, hdr, bbox, false, value );
    } else if ( colType == JAG_C_COL_TYPE_POLYGON3D ) {
	    JagParser::addPolygon3DData( pgon, sp, false );
		if ( pgon.linestr.size() < N ) return false;
		line.copyFrom( pgon.linestr[N-1], true );
		JagCGAL::getRingStr( line, hdr, bbox, true, value );
    } else {
		return false;
	}

	return true;
}

bool BinaryOpNode::doAllInnerRingN( const Jstr& mk, const Jstr& hdr, const Jstr &colType, 
								    int srid, const JagStrSplit &sp, const Jstr &carg,  Jstr &value )
{
	//prt(("s3420 doAllOuterRing() mk=[%s] colType=[%s] sp1.print(): \n", mk.c_str(), colType.c_str() ));
	//sp.print();
	int N = jagatoi( carg.c_str() ) + 1; // real ring number
	if ( N <= 1 ) return false;
	value = "";
	Jstr bbox;
	if ( mk == JAG_OJAG ) { bbox = sp[1]; } 

	JagLineString line;
	JagPolygon pgon;
    if ( colType == JAG_C_COL_TYPE_POLYGON ) {
	    JagParser::addPolygonData( pgon, sp, false );
		if ( pgon.linestr.size() < N ) return false;
		line.copyFrom( pgon.linestr[N-1], true );
		JagCGAL::getRingStr( line, hdr, bbox, false, value );
    } else if ( colType == JAG_C_COL_TYPE_POLYGON3D ) {
	    JagParser::addPolygon3DData( pgon, sp, false );
		if ( pgon.linestr.size() < N ) return false;
		line.copyFrom( pgon.linestr[N-1], true );
		JagCGAL::getRingStr( line, hdr, bbox, true, value );
    } else {
		return false;
	}

	return true;
}

bool BinaryOpNode::doAllPolygonN( const Jstr& mk, const Jstr& hdr, const Jstr &colType, 
								    const JagStrSplit &sp, const Jstr &carg, Jstr &value )
{
	prt(("s5420 doAllPolygonN() mk=[%s] colType=[%s] carg=[%s] sp1.print(): \n", mk.c_str(), carg.c_str(), colType.c_str() ));
	//sp.print();

	value = "";
	int N = jagatoi( carg.c_str() );
	if ( N <= 0 ) return false;

	Jstr bbox;
	if ( mk == JAG_OJAG ) { bbox = sp[1]; } 
	JagVector<JagPolygon> pgvec;
	int rc;
    if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON ) {
	    JagParser::addMultiPolygonData( pgvec, sp, false, false );
		JagCGAL::getPolygonNStr( pgvec, hdr, bbox, false, N, value );
    } else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
	    JagParser::addMultiPolygonData( pgvec, sp, false, true );
		JagCGAL::getPolygonNStr( pgvec, hdr, bbox, true, N, value );
    } else {
		return false;
	}

	return true;
}

bool BinaryOpNode::doAllNumPolygons( const Jstr& mk, const Jstr& hdr, const Jstr &colType, 
								    const JagStrSplit &sp, Jstr &value )
{
	prt(("s3420 doAllNumPolygons() mk=[%s] colType=[%s] sp1.print(): \n", mk.c_str(), colType.c_str() ));
	//sp.print();
	int num;
	value = "0";
	int rc;
    if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON || colType == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		num = 1;
		for ( int i=JAG_SP_START; i < sp.length(); ++i ) {
			if ( sp[i] == "!" ) ++num;
		}
		value = intToStr( num );
	} else if ( colType == JAG_C_COL_TYPE_POLYGON || colType == JAG_C_COL_TYPE_POLYGON3D ) {
		value = "1"; 
    } else {
		return true;
	}

	return true;
}

bool BinaryOpNode::doAllUnique( const Jstr& mk, const Jstr& hdr, const Jstr &colType, 
								    const JagStrSplit &sp, Jstr &value )
{
	prt(("s3420 doAllUnique() mk=[%s] colType=[%s] sp1.print(): \n", mk.c_str(), colType.c_str() ));
	//sp.print();
	prt(("s3420 doAllUnique() hdr=[%s] src=[%s]\n", hdr.c_str(), sp.c_str() ));

	Jstr bbox = sp[1];
	value = "";
    if ( colType == JAG_C_COL_TYPE_POLYGON
	     || colType == JAG_C_COL_TYPE_POLYGON3D 
	     || colType == JAG_C_COL_TYPE_LINESTRING 
	     || colType == JAG_C_COL_TYPE_LINESTRING3D 
	     || colType == JAG_C_COL_TYPE_MULTILINESTRING
	     || colType == JAG_C_COL_TYPE_MULTILINESTRING3D 
	     || colType == JAG_C_COL_TYPE_MULTIPOLYGON
	     || colType == JAG_C_COL_TYPE_MULTIPOLYGON3D
		 ) {
		JagCGAL::getUniqueStr( sp, hdr, bbox, value );
    } else {
		value = sp.c_str();
	}

	return true;
}

// 2D only: point, line, linestring, polygon, multipoint, multilinestring, multipolygon
bool BinaryOpNode::doAllBuffer( const Jstr& mk, const Jstr& hdr, const Jstr &colType, 
								    int srid, const JagStrSplit &sp, const Jstr &carg,  Jstr &value )
{
	prt(("s3420 doAllBuffer() mk=[%s] colType=[%s] sp1.print(): \n", mk.c_str(), colType.c_str() ));
	//sp.print();
	value = "";

	/**
		point2D, linestring2D, Polygon2D, multiPoint2D, MultiLineString, MultiPolygon
	**/
	JagLineString line;
	JagPolygon pgon;
	JagVector<JagPolygon> pgvec;
	double px, py;
	bool rc;
	//prt(("s8830 JAG_OJAG\n" ));
	//sp.print();
    if ( colType == JAG_C_COL_TYPE_LINESTRING || colType == JAG_C_COL_TYPE_MULTIPOINT ) {
		JagParser::addLineStringData( line, sp );
    } else if ( colType == JAG_C_COL_TYPE_POLYGON ) {
	    JagParser::addPolygonData( pgon, sp, true );
		line.copyFrom( pgon.linestr[0], true );
    } else if ( colType == JAG_C_COL_TYPE_MULTILINESTRING ) {
	    JagParser::addPolygonData( pgon, sp, false );
    } else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		JagParser::addMultiPolygonData( pgvec, sp, false, false );
	} else {
	}


    if ( colType == JAG_C_COL_TYPE_POINT ) {
		px = jagatof( sp[JAG_SP_START+0] );
		py = jagatof( sp[JAG_SP_START+1] );
		line.add(px, py);
		rc = JagCGAL::getBufferMultiPoint2DStr( line, srid, carg, value );
	} else if ( colType == JAG_C_COL_TYPE_LINE ) {
		px = jagatof( sp[JAG_SP_START+0] );
		py = jagatof( sp[JAG_SP_START+1] );
		line.add(px, py);
		px = jagatof( sp[JAG_SP_START+2] );
		py = jagatof( sp[JAG_SP_START+3] );
		line.add(px, py);
		rc = JagCGAL::getBufferLineString2DStr( line, srid, carg, value );
	} else if ( colType == JAG_C_COL_TYPE_LINESTRING ) {
		rc = JagCGAL::getBufferLineString2DStr( line, srid, carg, value );
    } else if (  colType == JAG_C_COL_TYPE_MULTIPOINT ) {
		rc = JagCGAL::getBufferMultiPoint2DStr( line, srid, carg, value );
    } else if ( colType == JAG_C_COL_TYPE_POLYGON ) {
		rc = JagCGAL::getBufferPolygon2DStr( pgon, srid, carg, value );
    } else if ( colType == JAG_C_COL_TYPE_MULTILINESTRING ) {
		rc = JagCGAL::getBufferMultiLineString2DStr( pgon, srid, carg, value );
    } else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		rc = JagCGAL::getBufferMultiPolygon2DStr( pgvec, srid, carg, value );
    } else if ( colType == JAG_C_COL_TYPE_SQUARE ) {
		px = jagatof( sp[JAG_SP_START+0] );
		py = jagatof( sp[JAG_SP_START+1] );
		double a = jagatof( sp[JAG_SP_START+2] );
		double nx = jagatof( sp[JAG_SP_START+3] );
		JagSquare2D sq(px,py, a, nx );
		JagPolygon pgon( sq );
		rc = JagCGAL::getBufferPolygon2DStr( pgon, srid, carg, value );
    } else if ( colType == JAG_C_COL_TYPE_CIRCLE ) {
		px = jagatof( sp[JAG_SP_START+0] );
		py = jagatof( sp[JAG_SP_START+1] );
		double a = jagatof( sp[JAG_SP_START+2] );
		JagCircle2D cir(px,py, a );
		JagPolygon pgon( cir );
		rc = JagCGAL::getBufferPolygon2DStr( pgon, srid, carg, value );
    } else if ( colType == JAG_C_COL_TYPE_RECTANGLE ) {
		px = jagatof( sp[JAG_SP_START+0] );
		py = jagatof( sp[JAG_SP_START+1] );
		double a = jagatof( sp[JAG_SP_START+2] );
		double b = jagatof( sp[JAG_SP_START+3] );
		double nx = jagatof( sp[JAG_SP_START+4] );
		JagRectangle2D rect(px,py, a, b, nx );
		JagPolygon pgon( rect );
		rc = JagCGAL::getBufferPolygon2DStr( pgon, srid, carg, value );
    } else if ( colType == JAG_C_COL_TYPE_ELLIPSE ) {
		px = jagatof( sp[JAG_SP_START+0] );
		py = jagatof( sp[JAG_SP_START+1] );
		double a = jagatof( sp[JAG_SP_START+2] );
		double b = jagatof( sp[JAG_SP_START+3] );
		double nx = jagatof( sp[JAG_SP_START+4] );
		JagEllipse2D e(px,py, a, b, nx );
		JagPolygon pgon( e );
		rc = JagCGAL::getBufferPolygon2DStr( pgon, srid, carg, value );
    } else if ( colType == JAG_C_COL_TYPE_TRIANGLE ) {
		double x1 = jagatof( sp[JAG_SP_START+0] );
		double y1 = jagatof( sp[JAG_SP_START+1] );
		double x2 = jagatof( sp[JAG_SP_START+2] );
		double y2 = jagatof( sp[JAG_SP_START+3] );
		double x3 = jagatof( sp[JAG_SP_START+4] );
		double y3 = jagatof( sp[JAG_SP_START+5] );
		JagTriangle2D t(x1,y1, x2,y2, x3,y3 );
		JagPolygon pgon( t );
		rc = JagCGAL::getBufferPolygon2DStr( pgon, srid, carg, value );
	} else  {
		rc = false;
	}

	return rc;
}

bool BinaryOpNode::doAllEndPoint( const Jstr& mk, const Jstr &colType, const JagStrSplit &sp, Jstr &value )
{
	//prt(("s3420 doAllEndPoint() colType1=[%s] carg=[%s] sp1.print(): \n", colType1.c_str(), carg.c_str() ));
	value = "";
	if ( JagParser::isVectorGeoType( colType ) 
	     && colType != JAG_C_COL_TYPE_TRIANGLE 
	     && colType != JAG_C_COL_TYPE_TRIANGLE3D 
	   ) {
		 return false;
	 }

	//sp.print();
	//prt(("s3653 sp.length()=%d\n", sp.length() ));

	if ( colType == JAG_C_COL_TYPE_LINE ) {
    	value = trimEndZeros(sp[2]) + " " + trimEndZeros(sp[3]);
	} else if ( colType == JAG_C_COL_TYPE_LINE3D  ) {
    	value = trimEndZeros(sp[3]) + " " + trimEndZeros(sp[4]) + " " + trimEndZeros(sp[5]);
	} else if ( colType == JAG_C_COL_TYPE_TRIANGLE ) {
		value = trimEndZeros(sp[4]) + " " + trimEndZeros(sp[5]);
	} else if ( colType == JAG_C_COL_TYPE_TRIANGLE3D ) {
		value = trimEndZeros(sp[6]) + " " + trimEndZeros(sp[7]) + " " + trimEndZeros(sp[8]);
	} else {
		int len = sp.length();
		value = sp[len-1];
		value.replace(':', ' ' );
	}

	//prt(("s2035 sp[0]=[%s]\n", sp[0].c_str() ));
	//prt(("s2039 sp[1]=[%s]\n", sp[1].c_str() ));
	//prt(("s2411 colType=[%s] value=[%s]\n",  colType.c_str(), value.c_str() ));
	return true;
}

bool BinaryOpNode::doAllIsRing( const Jstr& mk, const Jstr &colType, const JagStrSplit &sp, Jstr &value )
{
	//prt(("s3420 doAllIsClosed() colType1=[%s] carg=[%s] sp1.print(): \n", colType1.c_str(), carg.c_str() ));
	if ( JagParser::isVectorGeoType( colType ) ) {
		value = "1";
		return true;
	}

	bool rc;
    if ( colType == JAG_C_COL_TYPE_LINESTRING ) {
		JagLineString line;
		JagParser::addLineStringData( line, sp );
		rc = JagCGAL::getIsRingLineString2DStr( line );
    } else {
		rc = false;
	}
	
	if ( rc ) value = "1";
	else value = "0";
	return true;
}

bool BinaryOpNode::doAllIsPolygonCCW( const Jstr& mk, const Jstr &colType, const JagStrSplit &sp, Jstr &value )
{
    if ( colType != JAG_C_COL_TYPE_POLYGON ) {
		value = "0";
		return true;
	}
	bool rc = JagGeo::isPolygonCCW( sp );
	if ( rc ) value = "1";
	else value = "0";
	return true;
}

bool BinaryOpNode::doAllIsPolygonCW( const Jstr& mk, const Jstr &colType, const JagStrSplit &sp, Jstr &value )
{
    if ( colType != JAG_C_COL_TYPE_POLYGON ) {
		value = "0";
		return true;
	}
	bool rc = JagGeo::isPolygonCW( sp );
	if ( rc ) value = "1";
	else value = "0";
	return true;
}


bool BinaryOpNode::doAllIsValid( const Jstr& mk, const Jstr &colType, const JagStrSplit &sp, Jstr &value )
{
	//prt(("s3420 doAllIsClosed() colType1=[%s] carg=[%s] sp1.print(): \n", colType1.c_str(), carg.c_str() ));
	if ( JagParser::isVectorGeoType( colType ) ) {
		value = "1";
		return true;
	}

	bool rc;

	rc = false;
    if ( colType == JAG_C_COL_TYPE_LINESTRING ) {
		JagLineString line;
		JagParser::addLineStringData( line, sp );
		rc = JagCGAL::getIsValidLineString2DStr( line );
    } else if ( colType == JAG_C_COL_TYPE_POLYGON ) {
		JagPolygon pgon;
	    JagParser::addPolygonData( pgon, sp, true );
		rc = JagCGAL::getIsValidPolygon2DStr( pgon );
    } else if ( colType == JAG_C_COL_TYPE_MULTILINESTRING ) {
		JagPolygon pgon;
	    JagParser::addPolygonData( pgon, sp, false );
		rc = JagCGAL::getIsValidMultiLineString2DStr( pgon );
    } else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		JagVector<JagPolygon> pgvec;
		JagParser::addMultiPolygonData( pgvec, sp, false, false );
		rc = JagCGAL::getIsValidMultiPolygon2DStr( pgvec );
    } else if ( colType == JAG_C_COL_TYPE_MULTIPOINT ) {
		JagLineString line;
		JagParser::addLineStringData( line, sp );
		rc = JagCGAL::getIsValidMultiPoint2DStr( line );
	} else  {
	}

	if ( rc ) value = "1";
	else value = "0";
	return true;
}

bool BinaryOpNode::doAllIsSimple( const Jstr& mk, const Jstr &colType, const JagStrSplit &sp, Jstr &value )
{
	//prt(("s3420 doAllIsClosed() colType1=[%s] carg=[%s] sp1.print(): \n", colType1.c_str(), carg.c_str() ));
	if ( JagParser::isVectorGeoType( colType ) ) {
		value = "1";
		return true;
	}

	bool rc;

	rc = false;
    if ( colType == JAG_C_COL_TYPE_LINESTRING ) {
		JagLineString line;
		JagParser::addLineStringData( line, sp );
		rc = JagCGAL::getIsSimpleLineString2DStr( line );
    } else if ( colType == JAG_C_COL_TYPE_POLYGON ) {
		JagPolygon pgon;
	    JagParser::addPolygonData( pgon, sp, true );
		rc = JagCGAL::getIsSimplePolygon2DStr( pgon );
    } else if ( colType == JAG_C_COL_TYPE_MULTILINESTRING ) {
		JagPolygon pgon;
	    JagParser::addPolygonData( pgon, sp, false );
		rc = JagCGAL::getIsSimpleMultiLineString2DStr( pgon );
    } else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		JagVector<JagPolygon> pgvec;
		JagParser::addMultiPolygonData( pgvec, sp, false, false );
		rc = JagCGAL::getIsSimpleMultiPolygon2DStr( pgvec );
	} else  {
	}

	if ( rc ) value = "1";
	else value = "0";
	return true;
}

bool BinaryOpNode::doAllIsConvex( const Jstr& mk, const Jstr &colType, const JagStrSplit &sp, Jstr &value )
{
	//prt(("s3420 doAllIsClosed() colType1=[%s] carg=[%s] sp1.print(): \n", colType1.c_str(), carg.c_str() ));
	if ( JagParser::isVectorGeoType( colType ) ) {
		value = "1";
		return true;
	}

	bool rc = false;
    if ( colType == JAG_C_COL_TYPE_POLYGON ) {
		JagPolygon pgon;
	    JagParser::addPolygonData( pgon, sp, true );
		//rc = JagCGAL::getIsSimplePolygon2DStr( pgon );
		rc = JagCGAL::isPolygonConvex( pgon );
	} else  {
	}

	if ( rc ) value = "1";
	else value = "0";
	return true;
}

bool BinaryOpNode::doAllIsClosed( const Jstr& mk, const Jstr &colType, const JagStrSplit &sp, Jstr &value )
{
	//prt(("s3420 doAllIsClosed() colType1=[%s] carg=[%s] sp1.print(): \n", colType1.c_str(), carg.c_str() ));
	value = "0";
	if ( colType == JAG_C_COL_TYPE_LINE || colType == JAG_C_COL_TYPE_LINE3D ) {
		return true;
	}

	if ( colType == JAG_C_COL_TYPE_POLYGON || colType == JAG_C_COL_TYPE_POLYGON3D ) {
		value = "1";
		return true;
	}

	if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON || colType == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		value = "1";
		return true;
	}

	if ( JagParser::isVectorGeoType( colType ) ) {
		value = "1";
		return true;
	}

	if ( colType == JAG_C_COL_TYPE_LINESTRING || colType == JAG_C_COL_TYPE_LINESTRING3D 
	     ||  colType == JAG_C_COL_TYPE_MULTIPOINT || colType == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		// get first point, last point
		Jstr first, last;
    	JagStrSplit s( sp[1], ':' );
    	if ( s.length() < 4 ) {
    		first = sp[1]; 
    	} else {
    		first = JagGeo::safeGetStr(sp, 1);
    	}
		int len = sp.length();
		last = sp[len-1];
		if ( first == last ) {
			value = "1";
			return true;
		} else {
			value = "0";
			return true;
		}
	}

	value = "0";
	return true;
}

bool BinaryOpNode::doAllNumPoints( const Jstr& mk, const Jstr &colType, const JagStrSplit &sp, Jstr &value )
{
	//prt(("s3420 doAllNumPoints() colType=[%s] sp.print(): \n", colType.c_str() ));
	//sp.print();
	value = "0";

	if ( colType == JAG_C_COL_TYPE_POINT || colType == JAG_C_COL_TYPE_POINT3D ) {
		value = "1";
		return true;
	}

	if ( colType == JAG_C_COL_TYPE_LINE || colType == JAG_C_COL_TYPE_LINE3D ) {
		value = "2";
		return true;
	}

	if ( colType == JAG_C_COL_TYPE_TRIANGLE || colType == JAG_C_COL_TYPE_TRIANGLE3D ) {
		value = "3";
		return true;
	}

	if ( JagParser::isVectorGeoType( colType ) ) {
		value = "0";
		return true;
	}

	/**
	value = intToStr( sp.length() - 2 );
	prt(("s5029 value=[%s]\n", value.c_str() ));
	***/
	int cnt = 0;
	for ( int i=JAG_SP_START; i < sp.size(); ++i ) {
		if ( sp[i] != "|" && sp[i] != "!" ) {
			++cnt;
		}
	}
	value = intToStr( cnt );
	prt(("s5029 value=[%s]\n", value.c_str() ));
	return true;
}

bool BinaryOpNode::doAllNumSegments( const Jstr& mk, const Jstr &colType, const JagStrSplit &sp, Jstr &value )
{
	//prt(("s3420 doAllNumPoints() colType=[%s] sp.print(): \n", colType.c_str() ));
	//sp.print();
	value = "0";
	if ( colType == JAG_C_COL_TYPE_LINE || colType == JAG_C_COL_TYPE_LINE3D ) {
		value = "1";
		return true;
	}

	if ( colType == JAG_C_COL_TYPE_TRIANGLE || colType == JAG_C_COL_TYPE_TRIANGLE3D ) {
		value = "3";
		return true;
	}

	if ( colType == JAG_C_COL_TYPE_SQUARE || colType == JAG_C_COL_TYPE_SQUARE3D ||
	      colType == JAG_C_COL_TYPE_RECTANGLE || colType == JAG_C_COL_TYPE_RECTANGLE3D) {
		value = "4";
		return true;
	}

	if ( JagParser::isVectorGeoType( colType ) ) {
		value = "0";
		return true;
	}

	if ( colType == JAG_C_COL_TYPE_LINESTRING || colType == JAG_C_COL_TYPE_LINESTRING3D
		|| colType == JAG_C_COL_TYPE_POLYGON || colType == JAG_C_COL_TYPE_POLYGON3D 
		|| colType == JAG_C_COL_TYPE_MULTIPOLYGON || colType == JAG_C_COL_TYPE_MULTIPOLYGON3D 
	    || colType == JAG_C_COL_TYPE_MULTILINESTRING || colType == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
	    int n = JagGeo::numberOfSegments( sp ); 
		value = intToStr( n );
		prt(("s5029 value=[%s]\n", value.c_str() ));
		return true;
	}

	value = "0";
	return true;
}

bool BinaryOpNode::doAllNumRings( const Jstr& mk, const Jstr &colType, const JagStrSplit &sp, Jstr &value )
{
	if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON 
	    || colType == JAG_C_COL_TYPE_MULTIPOLYGON3D 
		|| colType == JAG_C_COL_TYPE_POLYGON
		|| colType == JAG_C_COL_TYPE_POLYGON3D ) {
		int cnt = 1;
		for ( int i =0; i < sp.length(); ++i ) {
			if ( sp[i] == "|" || sp[i] == "!" ) ++cnt;
		}
		value = intToStr(cnt);
		return true;
	}
	 
	value = "0";
	return true;
}

bool BinaryOpNode::doAllNumInnerRings( const Jstr& mk, const Jstr &colType, const JagStrSplit &sp, Jstr &value )
{
	if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON 
	    || colType == JAG_C_COL_TYPE_MULTIPOLYGON3D 
		|| colType == JAG_C_COL_TYPE_POLYGON
		|| colType == JAG_C_COL_TYPE_POLYGON3D ) {
		int cnt = 0;
		for ( int i =0; i < sp.length(); ++i ) {
			if ( sp[i] == "|" ) ++cnt;
		}
		value = intToStr(cnt);
		return true;
	}
	 
	value = "0";
	return true;
}

bool BinaryOpNode::doAllSummary( const Jstr& mk, const Jstr &colType, int srid, const JagStrSplit &sp, Jstr &value )
{
	Jstr npoints, nrings, isclosed;
	doAllNumPoints( mk, colType, sp, npoints );
	doAllNumRings( mk, colType, sp, nrings );
	doAllIsClosed( mk, colType, sp, isclosed );

	value = Jstr("geotype:") + JagGeo::getTypeStr(colType);
	value += Jstr(" srid:") + intToStr(srid);
	value += Jstr(" dimension:") +  intToStr(JagGeo::getPolyDimension( colType ) );
	value += Jstr(" numpoints:") + npoints;
	value += Jstr(" numrings:") + nrings;
	value += Jstr(" isclosed:") + isclosed;
	return true;
}

bool BinaryOpNode::doAllVolume( const Jstr& mk1, const Jstr &colType1, int srid1, 
									 const JagStrSplit &sp1, double &value )
{
	//prt(("s2315 colType1=[%s] \n", colType1.c_str() ));

	if ( colType1 == JAG_C_COL_TYPE_POINT ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_POINT3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_SPHERE ) {
		value = JagGeo::doSphereVolume(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_CUBE ) {
		value = JagGeo::doCubeVolume(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_BOX ) {
		value = JagGeo::doBoxVolume(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_CYLINDER ) {
		value = JagGeo::doCylinderVolume(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_CONE ) {
		value = JagGeo::doConeVolume(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSOID ) {
		value = JagGeo::doEllipsoidVolume(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_LINE ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_LINE3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		return false;
	} else { 
		return false;
	} 

	// prt(("s2411 colType1=[%s] not handled, false\n", colType1.c_str() ));
	return false;
}

bool BinaryOpNode::doAllArea( const Jstr& mk1, const Jstr &colType1, int srid1, 
									 const JagStrSplit &sp1, double &value )
{
	//prt(("s2315 colType1=[%s] \n", colType1.c_str() ));

	if ( colType1 == JAG_C_COL_TYPE_POINT ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_POINT3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE ) {
		value = JagGeo::doCircleArea(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE3D ) {
		value = JagGeo::doCircle3DArea(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_SPHERE ) {
		value = JagGeo::doSphereArea(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE ) {
		value = JagGeo::doSquareArea(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE3D ) {
		value = JagGeo::doSquare3DArea(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_CUBE ) {
		value = JagGeo::doCubeArea(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE ) {
		value = JagGeo::doRectangleArea(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE3D ) {
		value = JagGeo::doRectangle3DArea(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_BOX ) {
		value = JagGeo::doBoxArea(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE ) {
		value = JagGeo::doTriangleArea(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE3D ) {
		value = JagGeo::doTriangle3DArea(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_CYLINDER ) {
		value = JagGeo::doCylinderArea(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_CONE ) {
		value = JagGeo::doConeArea(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE ) {
		value = JagGeo::doEllipseArea(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSOID ) {
		value = JagGeo::doEllipsoidArea(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_LINE ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_LINE3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		value = JagGeo::doPolygonArea(  mk1, srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		value = JagGeo::doMultiPolygonArea( mk1, srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		return false;
	} else { 
		return false;
	} 

	// prt(("s2411 colType1=[%s] not handled, false\n", colType1.c_str() ));
	return false;
}


bool BinaryOpNode::doAllPerimeter( const Jstr& mk1, const Jstr &colType1, int srid1, 
									 const JagStrSplit &sp1, double &value )
{
	//prt(("s2315 colType1=[%s] \n", colType1.c_str() ));

	if ( colType1 == JAG_C_COL_TYPE_POINT ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_POINT3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE ) {
		value = JagGeo::doCirclePerimeter(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE3D ) {
		value = JagGeo::doCircle3DPerimeter(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_SPHERE ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE ) {
		value = JagGeo::doSquarePerimeter(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE3D ) {
		value = JagGeo::doSquare3DPerimeter(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_CUBE ) {
		value = JagGeo::doCubePerimeter(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE ) {
		value = JagGeo::doRectanglePerimeter(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE3D ) {
		value = JagGeo::doRectangle3DPerimeter(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_BOX ) {
		value = JagGeo::doBoxPerimeter(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE ) {
		value = JagGeo::doTrianglePerimeter(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE3D ) {
		value = JagGeo::doTriangle3DPerimeter(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_CYLINDER ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_CONE ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE ) {
		value = JagGeo::doEllipsePerimeter(  srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSOID ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_LINE ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_LINE3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		value = JagGeo::doPolygonPerimeter(  mk1, srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON3D ) {
		value = JagGeo::doPolygon3DPerimeter(  mk1, srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		return false;
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		value = JagGeo::doMultiPolygonPerimeter( mk1, srid1, sp1 );
		return true;
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		value = JagGeo::doMultiPolygon3DPerimeter( mk1, srid1, sp1 );
		return true;
	} else { 
		return false;
	} 

	// prt(("s2411 colType1=[%s] not handled, false\n", colType1.c_str() ));
	return false;
}

bool BinaryOpNode::doAllMinMax( int op, const Jstr& mk, const Jstr &colType, const JagStrSplit &sp, double &value )
{
	prt(("s2815 colType=[%s] sp.print(): \n", colType.c_str() ));
	//sp.print();

	// prt(("s2411 colType1=[%s] not handled, false\n", colType1.c_str() ));
	value = 0.0;
	bool rc = false;
	if ( JagParser::isVectorGeoType( colType ) ) {
		// todo for vector shapes, you can get min max coordinates by calculation
		return rc;
	}

	if ( mk == JAG_OJAG ) {
    	int len;
    	for ( int i=0; i < sp.length(); ++i ) {
    		JagStrSplit ss(sp[i], ':');
    		len = ss.length();
    		if ( len < 4 ) continue;
    		if ( len == 4 ) {
    			// xmin:ymin:xmax:ymax
    			if (  op == JAG_FUNC_XMIN ) {
    				value = jagatof( ss[0].c_str() );
    			} else if ( op == JAG_FUNC_YMIN ) {
    				value = jagatof( ss[1].c_str() );
    			} else if ( op == JAG_FUNC_XMAX ) {
    				value = jagatof( ss[2].c_str() );
    			} else if ( op == JAG_FUNC_YMAX ) {
    				value = jagatof( ss[3].c_str() );
    			} 
    		} else if ( len == 6 ) {
    			if (  op == JAG_FUNC_XMIN ) {
    				value = jagatof( ss[0].c_str() );
    			} else if ( op == JAG_FUNC_YMIN ) {
    				value = jagatof( ss[1].c_str() );
    			} else if ( op == JAG_FUNC_ZMIN ) {
    				value = jagatof( ss[2].c_str() );
    			} else if ( op == JAG_FUNC_XMAX ) {
    				value = jagatof( ss[3].c_str() );
    			} else if ( op == JAG_FUNC_YMAX ) {
    				value = jagatof( ss[4].c_str() );
    			} else if ( op == JAG_FUNC_ZMAX ) {
    				value = jagatof( ss[5].c_str() );
    			} 
    		}
    		rc = true;
    		break;
    	}
    	return rc;
	} else {
		// CJAG
		/**
		i=0 [CJAG=0=0=LS=0]
		i=1 [0:0]
		i=2 [2:3]
		i=3 [4:5]
		i=4 [9:3]
		i=5 [33:22]
		**/
		Jstr bbox;
		double xmin,ymin,xmax,ymax,zmin,zmax;
		int rc2;
		bool is3D = false;
		if ( colType == JAG_C_COL_TYPE_LINESTRING ) {
			JagLineString lstr;
			JagParser::addLineStringData( lstr, sp );
			lstr.bbox2D( xmin, ymin, xmax, ymax );
		} else if ( colType == JAG_C_COL_TYPE_LINESTRING3D ) {
			JagLineString3D lstr;
			JagParser::addLineString3DData( lstr, sp );
			lstr.bbox3D( xmin, ymin, zmin, xmax, ymax, zmax );
			is3D = true;
		} else if ( colType == JAG_C_COL_TYPE_POLYGON ) {
			JagPolygon pgon;
			rc2 = JagParser::addPolygonData( pgon, sp, true );
			if ( rc2 <= 0 ) return false; 
			pgon.bbox2D( xmin, ymin, xmax, ymax );
		} else if ( colType == JAG_C_COL_TYPE_POLYGON3D ) {
			JagPolygon pgon;
			rc2 = JagParser::addPolygon3DData( pgon, sp, true );
			if ( rc2 <= 0 ) return false;
			pgon.bbox3D( xmin, ymin, zmin, xmax, ymax, zmax );
			is3D = true;
		} else if ( colType == JAG_C_COL_TYPE_MULTILINESTRING ) {
			JagPolygon pgon;
			rc2 = JagParser::addPolygonData( pgon, sp, true );
			if ( rc2 <= 0 ) return false;
			pgon.bbox2D( xmin, ymin, xmax, ymax );
		} else if ( colType == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
			JagPolygon pgon;
			rc2 = JagParser::addPolygon3DData( pgon, sp, true );
			if ( rc2 <= 0 ) return false;
			pgon.bbox3D( xmin, ymin, zmin, xmax, ymax, zmax );
			is3D = true;
		} else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON ) {
			JagVector<JagPolygon> pgvec;
			rc2 = JagParser::addMultiPolygonData( pgvec, sp, true, false );
			if ( rc2 <= 0 ) return false;
			bool rb = JagGeo::getBBox2D( pgvec, xmin,ymin,xmax,ymax );
			if ( ! rb ) { return false; }
		} else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
			JagVector<JagPolygon> pgvec;
			rc2 = JagParser::addMultiPolygonData( pgvec, sp, true, true );
			if ( rc2 <= 0 ) return false;
			is3D = true;
			bool rb = JagGeo::getBBox3D( pgvec, xmin,ymin,zmin, xmax,ymax,zmax );
			if ( ! rb ) { return false; }
		} else {
			return false;
		}

    	if ( ! is3D ) {
    			// xmin:ymin:xmax:ymax
    			if (  op == JAG_FUNC_XMIN ) {
    				value = xmin;
    			} else if ( op == JAG_FUNC_YMIN ) {
    				value = ymin;
    			} else if ( op == JAG_FUNC_XMAX ) {
    				value = xmax;
    			} else if ( op == JAG_FUNC_YMAX ) {
    				value = ymax;
    			} 
   		} else {
    			if (  op == JAG_FUNC_XMIN ) {
    				value = xmin;
    			} else if ( op == JAG_FUNC_YMIN ) {
    				value = ymin;
    			} else if ( op == JAG_FUNC_ZMIN ) {
    				value = zmin;
    			} else if ( op == JAG_FUNC_XMAX ) {
    				value = xmax;
    			} else if ( op == JAG_FUNC_YMAX ) {
    				value = ymax;
    			} else if ( op == JAG_FUNC_ZMAX ) {
    				value = zmax;
    			} 
   		}

		rc = true;
		return rc;
	}
}

Jstr BinaryOpNode::doAllUnion( const Jstr& mark1, const Jstr &colType1, 
										int srid1, const JagStrSplit &sp1, const Jstr& mark2, 
										const Jstr &colType2, int srid2, const JagStrSplit &sp2 )
{
	prt(("s2410 doAllUnion colType1=[%s] colType2=[%s] \n", colType1.c_str(),  colType2.c_str() ));

	if ( colType1 == JAG_C_COL_TYPE_POINT ) {
		return JagGeo::doPointAddition(  srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_POINT3D ) {
		return JagGeo::doPoint3DAddition(  srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_LINE ) {
		return JagGeo::doLineAddition(  srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_LINE3D ) {
		return JagGeo::doLine3DAddition(  srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING ) {
		return JagGeo::doLineStringAddition( mark1, srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return JagGeo::doLineString3DAddition( mark1, srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		return JagGeo::doPolygonUnion( mark1, srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return JagGeo::doPolygonUnion( mark2, srid2, sp2, mark1, colType1, srid1, sp1 );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON3D ) {
		return JagGeo::doPolygon3DAddition( mark1, srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING ) {
		return JagGeo::doPolygonAddition( mark1, srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		return JagGeo::doPolygon3DAddition( mark1, srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		return JagGeo::doMultiPolygonAddition( mark1, srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		return JagGeo::doMultiPolygon3DAddition( mark1, srid1, sp1, mark2, colType2, srid2, sp2 );
	} else {
		return "";
	}
}

Jstr BinaryOpNode::doAllCollect( const Jstr& mark1, const Jstr &colType1, 
										int srid1, const JagStrSplit &sp1, const Jstr& mark2, 
										const Jstr &colType2, int srid2, const JagStrSplit &sp2 )
{
	//prt(("s2410 doAllCollect colType1=[%s] colType2=[%s] \n", colType1.c_str(),  colType2.c_str() ));

	if ( colType1 == JAG_C_COL_TYPE_POINT ) {
		return JagGeo::doPointAddition(  srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_POINT3D ) {
		return JagGeo::doPoint3DAddition(  srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_LINE ) {
		return JagGeo::doLineAddition(  srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_LINE3D ) {
		return JagGeo::doLine3DAddition(  srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING ) {
		return JagGeo::doLineStringAddition( mark1, srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return JagGeo::doLineString3DAddition( mark1, srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		return JagGeo::doPolygonAddition( mark1, srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON3D ) {
		return JagGeo::doPolygon3DAddition( mark1, srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING ) {
		return JagGeo::doPolygonAddition( mark1, srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		return JagGeo::doPolygon3DAddition( mark1, srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		return JagGeo::doMultiPolygonAddition( mark1, srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		return JagGeo::doMultiPolygon3DAddition( mark1, srid1, sp1, mark2, colType2, srid2, sp2 );
	} else {
		return "";
	}
}

Jstr BinaryOpNode::doAllIntersection( const Jstr& mark1, const Jstr &colType1, 
										const JagStrSplit &sp1, const Jstr& mark2, 
										const Jstr &colType2, const JagStrSplit &sp2 )
{
	prt(("s2410 doAllIntersection colType1=[%s] colType2=[%s] \n", colType1.c_str(),  colType2.c_str() ));

	if ( colType1 == JAG_C_COL_TYPE_POINT || colType1 == JAG_C_COL_TYPE_POINT3D ) {
		return JagGeo::doPointIntersection(  colType1,sp1, colType2,sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_POINT || colType2 == JAG_C_COL_TYPE_POINT3D ) {
		return JagGeo::doPointIntersection(  colType2,sp2, colType1,sp1 );
	} else if ( colType1 == JAG_C_COL_TYPE_LINE || colType1 == JAG_C_COL_TYPE_LINE3D ) {
		return JagGeo::doLineIntersection( colType1,sp1, colType2,sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_LINE || colType2 == JAG_C_COL_TYPE_LINE3D ) {
		return JagGeo::doLineIntersection( colType2,sp2, colType1,sp1 );
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING || colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return JagGeo::doLineStringIntersection( colType1,sp1, colType2,sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING || colType2 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return JagGeo::doLineStringIntersection( colType2,sp2, colType1,sp1 );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING || colType1 == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		return JagGeo::doMultiLineStringIntersection( colType1,sp1, colType2,sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_MULTILINESTRING || colType2 == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		return JagGeo::doMultiLineStringIntersection( colType2,sp2, colType1,sp1 );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		prt(("s2230 colType1 == JAG_C_COL_TYPE_POLYGON doPolygonIntersection ...\n" ));
		return JagGeo::doPolygonIntersection( colType1,sp1, colType2,sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		prt(("s2231 colType2 == JAG_C_COL_TYPE_POLYGON doPolygonIntersection ...\n" ));
		return JagGeo::doPolygonIntersection( colType2,sp2, colType1,sp1 );
	} else {
		return "";
	}
}

Jstr BinaryOpNode::doAllDifference( const Jstr& mark1, const Jstr &colType1, 
										const JagStrSplit &sp1, const Jstr& mark2, 
										const Jstr &colType2, const JagStrSplit &sp2 )
{
	prt(("s2410 doAllDifference colType1=[%s] colType2=[%s] \n", colType1.c_str(),  colType2.c_str() ));

	if ( colType1 == JAG_C_COL_TYPE_POINT || colType1 == JAG_C_COL_TYPE_POINT3D ) {
		return JagGeo::doPointDifference(  colType1,sp1, colType2,sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_LINE || colType1 == JAG_C_COL_TYPE_LINE3D ) {
		return JagGeo::doLineDifference( colType1,sp1, colType2,sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING || colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return JagGeo::doLineStringDifference( colType1,sp1, colType2,sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING || colType1 == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		return JagGeo::doMultiLineStringDifference( colType1,sp1, colType2,sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		return JagGeo::doPolygonDifference( colType1,sp1, colType2,sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		return JagGeo::doMultiPolygonDifference( colType1,sp1, colType2,sp2 );
	} else {
		return "";
	}
}

Jstr BinaryOpNode::doAllSymDifference( const Jstr& mark1, const Jstr &colType1, 
										const JagStrSplit &sp1, const Jstr& mark2, 
										const Jstr &colType2, const JagStrSplit &sp2 )
{
	prt(("s2410 doAllSymDifference colType1=[%s] colType2=[%s] \n", colType1.c_str(),  colType2.c_str() ));

	if ( colType1 == JAG_C_COL_TYPE_POINT || colType1 == JAG_C_COL_TYPE_POINT3D ) {
		return JagGeo::doPointSymDifference(  colType1,sp1, colType2,sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_POINT || colType2 == JAG_C_COL_TYPE_POINT3D ) {
		return JagGeo::doPointSymDifference(  colType2,sp2, colType1,sp1 );
	} else if ( colType1 == JAG_C_COL_TYPE_LINE || colType1 == JAG_C_COL_TYPE_LINE3D ) {
		return JagGeo::doLineSymDifference( colType1,sp1, colType2,sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_LINE || colType2 == JAG_C_COL_TYPE_LINE3D ) {
		return JagGeo::doLineSymDifference( colType2,sp2, colType1,sp1 );
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING || colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return JagGeo::doLineStringSymDifference( colType1,sp1, colType2,sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING || colType2 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return JagGeo::doLineStringSymDifference( colType2,sp2, colType1,sp1 );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING || colType1 == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		return JagGeo::doMultiLineStringSymDifference( colType1,sp1, colType2,sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_MULTILINESTRING || colType2 == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		return JagGeo::doMultiLineStringSymDifference( colType2,sp2, colType1,sp1 );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		return JagGeo::doPolygonSymDifference( colType1,sp1, colType2,sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return JagGeo::doPolygonSymDifference( colType2,sp2, colType1,sp1 );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		return JagGeo::doMultiPolygonSymDifference( colType1,sp1, colType2,sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		return JagGeo::doMultiPolygonSymDifference( colType2,sp2, colType1,sp1 );
	} else {
		return "";
	}
}

Jstr BinaryOpNode::doAllLocatePoint( int srid, const Jstr& mark1, const Jstr &colType1, 
										const JagStrSplit &sp1, const Jstr& mark2, 
										const Jstr &colType2, const JagStrSplit &sp2 )
{
	prt(("s2410 doAllLocatePoint colType1=[%s] colType2=[%s] \n", colType1.c_str(),  colType2.c_str() ));
	// locatepoint(lstr, point)

	if ( colType1 == JAG_C_COL_TYPE_POINT || colType1 == JAG_C_COL_TYPE_POINT3D ) {
		return JagGeo::doLocatePoint(  srid, colType2,sp2, colType1,sp1 );
	} else if ( colType2 == JAG_C_COL_TYPE_POINT || colType2 == JAG_C_COL_TYPE_POINT3D ) {
		return JagGeo::doLocatePoint(  srid, colType1,sp1, colType2,sp2 );
	} else if ( colType1 == JAG_C_COL_TYPE_LINE || colType1 == JAG_C_COL_TYPE_LINE3D ) {
		return JagGeo::doLocatePoint( srid, colType1,sp1, colType2,sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_LINE || colType2 == JAG_C_COL_TYPE_LINE3D ) {
		return JagGeo::doLocatePoint( srid, colType2,sp2, colType1,sp1 );
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING || colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return JagGeo::doLocatePoint( srid, colType1,sp1, colType2,sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING || colType2 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return JagGeo::doLocatePoint( srid, colType2,sp2, colType1,sp1 );
	} else {
		return "";
	}
}

Jstr BinaryOpNode::doAllAddPoint( int srid, const Jstr& mark1, const Jstr &colType1, 
										const JagStrSplit &sp1, const Jstr& mark2, 
										const Jstr &colType2, const JagStrSplit &sp2, const Jstr &carg )
{
	prt(("s2410 doAllLocatePoint colType1=[%s] colType2=[%s] \n", colType1.c_str(),  colType2.c_str() ));
	// locatepoint(lstr, point)
	int pos = jagatoi( carg.c_str() );
	if ( pos <= 0 ) pos = INT_MAX;

	Jstr val;
	if ( colType1 == JAG_C_COL_TYPE_LINESTRING || colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		if ( colType2 == JAG_C_COL_TYPE_POINT ) {
			val = Jstr("CJAG=0=0=LS=d 0:0:0:0");
			int current=1;
			for ( int i=JAG_SP_START; i < sp1.size(); ++i ) {
				if ( current == pos ) {
					val += Jstr(" ") + sp2[JAG_SP_START+0] + ":" + sp2[JAG_SP_START+1];
				} 
				val += Jstr(" ") + sp1[i];
				++current;
			}
			if ( INT_MAX == pos ) {
				val += Jstr(" ") + sp2[JAG_SP_START+0] + ":" + sp2[JAG_SP_START+1];
			}
		} else if ( colType2 == JAG_C_COL_TYPE_POINT3D ) {
			val = Jstr("CJAG=0=0=LS3=d 0:0:0:0:0:0");
			int current=1;
			for ( int i=JAG_SP_START; i < sp1.size(); ++i ) {
				if ( current == pos ) {
					val += Jstr(" ") + sp2[JAG_SP_START+0] + ":" + sp2[JAG_SP_START+1] + ":" + sp2[JAG_SP_START+2];
				} 
				val += Jstr(" ") + sp1[i];
				++current;
			}
			if ( INT_MAX == pos ) {
				val += Jstr(" ") + sp2[JAG_SP_START+0] + ":" + sp2[JAG_SP_START+1] + ":" + sp2[JAG_SP_START+2];
			}
		}
	} 
	return val;
}

Jstr BinaryOpNode::doAllSetPoint( int srid, const Jstr& mark1, const Jstr &colType1, 
										const JagStrSplit &sp1, const Jstr& mark2, 
										const Jstr &colType2, const JagStrSplit &sp2, const Jstr &carg )
{
	if ( sp1.size() < 1 ) return "";
	int pos = jagatoi( carg.c_str() );
	if ( pos <= 0 ) pos = INT_MAX;

	Jstr val;
	if ( colType1 == JAG_C_COL_TYPE_LINESTRING || colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		if ( colType2 == JAG_C_COL_TYPE_POINT ) {
			val = Jstr("CJAG=0=0=LS=d 0:0:0:0");
			int current=1;
			for ( int i=JAG_SP_START; i < sp1.size(); ++i ) {
				if ( current == pos ) {
					val += Jstr(" ") + sp2[JAG_SP_START+0] + ":" + sp2[JAG_SP_START+1];
				} else { 
					val += Jstr(" ") + sp1[i];
				}
				++current;
			}
		} else if ( colType2 == JAG_C_COL_TYPE_POINT3D ) {
			val = Jstr("CJAG=0=0=LS3=d 0:0:0:0:0:0");
			int current=1;
			for ( int i=JAG_SP_START; i < sp1.size(); ++i ) {
				if ( current == pos ) {
					val += Jstr(" ") + sp2[JAG_SP_START+0] + ":" + sp2[JAG_SP_START+1] + ":" + sp2[JAG_SP_START+2];
				} else {
					val += Jstr(" ") + sp1[i];
				}
				++current;
			}
		}
	} 
	return val;
}

bool BinaryOpNode::doAllRemovePoint( int srid, const Jstr &colType1, 
									const JagStrSplit &sp1, const Jstr &carg, Jstr &val )
{
	if ( sp1.size() < 1 ) return false;
	int pos = jagatoi( carg.c_str() );
	if ( pos <= 0 ) pos = INT_MAX;

	if ( colType1 == JAG_C_COL_TYPE_LINESTRING  ) {
		val = Jstr("CJAG=0=0=LS=d 0:0:0:0");
	} else {
		val = Jstr("CJAG=0=0=LS3=d 0:0:0:0:0:0");
	}

	int current=1;
	for ( int i=JAG_SP_START; i < sp1.size(); ++i ) {
		if ( current != pos ) {
			val += Jstr(" ") + sp1[i];
		}
		++current;
	}

	return true;
}

Jstr BinaryOpNode::doAllScaleAt( int srid, const Jstr& mark1, const Jstr &colType1, 
										const JagStrSplit &sp1, const Jstr& mark2, 
										const Jstr &colType2, const JagStrSplit &sp2, const Jstr &carg )
{
	prt(("s1727 doAllScaleAt sp1.size()=%d\n", sp1.size() ));
	if ( sp1.size() < 1 ) return false;
	double fx, fy, fz;
	fx = fy = fz = 1.0;
	JagStrSplit sf( carg, ':', true);
	if ( 3 == sf.size() ) {
		fx = fabs(sf[0].tof()); fy = fabs(sf[1].tof()); fz = fabs(sf[2].tof());
	} else if ( 2 == sf.size() ) {
		fx = fabs(sf[0].tof()); fy = fabs(sf[1].tof()); 
	} else {
		fx = fy = fz = fabs(carg.tof());
	}
	prt(("s1827 doAllScaleAt fx=%.2f fy=%.2f fz=%.2f colType1=%s\n", fx, fy, fz, colType1.c_str() ));
	double px, py, pz, x, y, z;
	px = sp2[JAG_SP_START+0].tof();
	py = sp2[JAG_SP_START+1].tof();
	pz = sp2[JAG_SP_START+2].tof();
	Jstr val;

	if ( colType1 == JAG_C_COL_TYPE_POINT ) {
		double x,y;
		val = "CJAG=" + intToStr(srid) + "=0=PT=d 0:0:0:0 ";
		x = sp1[JAG_SP_START+0].tof(); y = sp1[JAG_SP_START+1].tof();
		x = px + fx*(x-px); y = py + fy*(y-py);
		val += d2s(x) + Jstr(" ") + d2s(y);
	} else if ( colType1 == JAG_C_COL_TYPE_POINT3D ) {
		double x,y,z;
		val = "CJAG=" + intToStr(srid) + "=0=PT=d 0:0:0:0 ";
		x = sp1[JAG_SP_START+0].tof(); y = sp1[JAG_SP_START+1].tof(); z = sp1[JAG_SP_START+2].tof();
		x = px + fx*(x-px); y = py + fy*(y-py); z = pz + fz*(z-pz);
		val += d2s(x) + Jstr(" ") + d2s(y) + " " + d2s(z);
	} else if ( colType1 == JAG_C_COL_TYPE_LINE  ) {
		val = "CJAG=" + intToStr(srid) + "=0=LN=d 0:0:0:0 ";
		x = sp1[JAG_SP_START+0].tof(); y = sp1[JAG_SP_START+1].tof();
		x = px + fx*(x-px); y = py + fy*(y-py);
		val += d2s(x) + Jstr(" ") + d2s(y);

		x = sp1[JAG_SP_START+2].tof(); y = sp1[JAG_SP_START+3].tof();
		x = px + fx*(x-px); y = py + fy*(y-py);
		val += d2s(x) + Jstr(" ") + d2s(y);
	} else if ( colType1 == JAG_C_COL_TYPE_LINE3D ) {
		val = "CJAG=" + intToStr(srid) + "=0=LN3=d 0:0:0:0:0:0 ";
		x = sp1[JAG_SP_START+0].tof(); y = sp1[JAG_SP_START+1].tof(); z = sp1[JAG_SP_START+2].tof();
		x = px + fx*(x-px); y = py + fy*(y-py); z = pz + fz*(z-pz);
		val += d2s(x) + Jstr(" ") + d2s(y) + " " + d2s(z);

		x = sp1[JAG_SP_START+3].tof(); y = sp1[JAG_SP_START+4].tof(); z = sp1[JAG_SP_START+5].tof();
		x = px + fx*(x-px); y = py + fy*(y-py); z = pz + fz*(z-pz);
		val += d2s(x) + Jstr(" ") + d2s(y) + " " + d2s(z);
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE  ) {
		val = "CJAG=" + intToStr(srid) + "=0=TR=d 0:0:0:0 ";
		// 3 pairs of x-y coords
		x = sp1[JAG_SP_START+0].tof(); y = sp1[JAG_SP_START+1].tof();
		x = px + fx*(x-px); y = py + fy*(y-py);
		val += d2s(x) + Jstr(" ") + d2s(y);

		x = sp1[JAG_SP_START+2].tof(); y = sp1[JAG_SP_START+3].tof();
		x = px + fx*(x-px); y = py + fy*(y-py);
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);

		x = sp1[JAG_SP_START+4].tof(); y = sp1[JAG_SP_START+5].tof();
		x = px + fx*(x-px); y = py + fy*(y-py);
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE3D  ) {
		val = "CJAG=" + intToStr(srid) + "=0=TR3=d 0:0:0:0:0:0 ";
		x = sp1[JAG_SP_START+0].tof(); y = sp1[JAG_SP_START+1].tof(); z = sp1[JAG_SP_START+2].tof();
		x = px + fx*(x-px); y = py + fy*(y-py); z = pz + fz*(z-pz);
		val += d2s(x) + Jstr(" ") + d2s(y) + " " + d2s(z);

		x = sp1[JAG_SP_START+3].tof(); y = sp1[JAG_SP_START+4].tof(); z = sp1[JAG_SP_START+5].tof();
		x = px + fx*(x-px); y = py + fy*(y-py); z = pz + fz*(z-pz);
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y) + " " + d2s(z);

		x = sp1[JAG_SP_START+6].tof(); y = sp1[JAG_SP_START+7].tof(); z = sp1[JAG_SP_START+8].tof();
		x = px + fx*(x-px); y = py + fy*(y-py); z = pz + fz*(z-pz);
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y) + " " + d2s(z);
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		x = px + fx*(x-px); y = py + fy*(y-py);
		double r = jagatof( sp1[JAG_SP_START+2].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+3);
		double a = r *fx;
		double b = r *fy;
		val = "CJAG=" + intToStr(srid) + "=0=RC=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE3D ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )*fx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )*fy;
		double z = jagatof( sp1[JAG_SP_START+2].c_str() )*fz;
		double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+4);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+5);
		x = px + fx*(x-px); y = py + fy*(y-py); z = pz + fz*(z-pz);
		double a = r *fx;
		double b = r *fy;
		val = "CJAG=" + intToStr(srid) + "=0=RC3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double a = jagatof( sp1[JAG_SP_START+2].c_str() )*fx;
		double b = jagatof( sp1[JAG_SP_START+3].c_str() )*fy;
		double nx = JagGeo::safeget(sp1, JAG_SP_START+3);
		x = px + fx*(x-px); y = py + fy*(y-py);
		val = "CJAG=" + intToStr(srid) + "=0=RC=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE3D ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double z = jagatof( sp1[JAG_SP_START+2].c_str() );
		double a = jagatof( sp1[JAG_SP_START+3].c_str() )*fx;
		double b = jagatof( sp1[JAG_SP_START+4].c_str() )*fy;
		double nx = JagGeo::safeget(sp1, JAG_SP_START+5);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+6);
		x = px + fx*(x-px); y = py + fy*(y-py); z = pz + fz*(z-pz);
		val = "CJAG=" + intToStr(srid) + "=0=RC3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double r = jagatof( sp1[JAG_SP_START+2].c_str() );
		double a = r *fx;
		double b = r *fy;
		x = px + fx*(x-px); y = py + fy*(y-py);
		val = "CJAG=" + intToStr(srid) + "=0=EL=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " 0.0";
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE3D ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double z = jagatof( sp1[JAG_SP_START+2].c_str() );
		double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+4);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+5);
		double a = r *fx;
		double b = r *fy;
		x = px + fx*(x-px); y = py + fy*(y-py); z = pz + fz*(z-pz);
		val = "CJAG=" + intToStr(srid) + "=0=EL3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx) + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double a = jagatof( sp1[JAG_SP_START+2].c_str() )*fx;
		double b = jagatof( sp1[JAG_SP_START+3].c_str() )*fy;
		double nx = JagGeo::safeget(sp1, JAG_SP_START+3);
		x = px + fx*(x-px); y = py + fy*(y-py);
		val = "CJAG=" + intToStr(srid) + "=0=EL=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE3D ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double z = jagatof( sp1[JAG_SP_START+2].c_str() );
		double a = jagatof( sp1[JAG_SP_START+3].c_str() )*fx;
		double b = jagatof( sp1[JAG_SP_START+4].c_str() )*fy;
		double nx = JagGeo::safeget(sp1, JAG_SP_START+5);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+6);
		x = px + fx*(x-px); y = py + fy*(y-py); z = pz + fz*(z-pz);
		val = "CJAG=" + intToStr(srid) + "=0=EL3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_CUBE  ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double z = jagatof( sp1[JAG_SP_START+2].c_str() );
		double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = jagatof( sp1[JAG_SP_START+4].c_str() );
		double ny = jagatof( sp1[JAG_SP_START+5].c_str() );
		double a = r *fx;
		double b = r *fy;
		double c = r *fz;
		x = px + fx*(x-px); y = py + fy*(y-py); z = pz + fz*(z-pz);
		val = "CJAG=" + intToStr(srid) + "=0=BX=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(c) 
				+ " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double z = jagatof( sp1[JAG_SP_START+2].c_str() );
		double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double a = r *fx;
		double b = r *fy;
		double c = r *fz;
		x = px + fx*(x-px); y = py + fy*(y-py); z = pz + fz*(z-pz);
		val = "CJAG=" + intToStr(srid) + "=0=ES=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(c) + " 0.0 0.0";
	} else if ( colType1 == JAG_C_COL_TYPE_BOX || colType1 == JAG_C_COL_TYPE_ELLIPSOID  ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double z = jagatof( sp1[JAG_SP_START+2].c_str() );
		double a = jagatof( sp1[JAG_SP_START+3].c_str() )*fx;
		double b = jagatof( sp1[JAG_SP_START+4].c_str() )*fy;
		double c = jagatof( sp1[JAG_SP_START+5].c_str() )*fz;
		double nx = jagatof( sp1[JAG_SP_START+6].c_str() );
		double ny = jagatof( sp1[JAG_SP_START+7].c_str() );
		x = px + fx*(x-px); y = py + fy*(y-py); z = pz + fz*(z-pz);
		if ( colType1 == JAG_C_COL_TYPE_BOX ) {
			val = "CJAG=" + intToStr(srid) + "=0=BX=d 0:0:0:0:0:0 ";
		} else {
			val = "CJAG=" + intToStr(srid) + "=0=ES=d 0:0:0:0:0:0 ";
		}
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(c) 
				+ " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_CONE || colType1 == JAG_C_COL_TYPE_CYLINDER  ) {
		if ( JagGeo::jagEQ(fx,fy) && JagGeo::jagEQ(fx,fz) ) {
            double x = jagatof( sp1[JAG_SP_START+0].c_str() );
            double y = jagatof( sp1[JAG_SP_START+1].c_str() );
            double z = jagatof( sp1[JAG_SP_START+2].c_str() );
            double r = jagatof( sp1[JAG_SP_START+3].c_str() )*fx;
            double h = jagatof( sp1[JAG_SP_START+4].c_str() )*fx;
            double nx = JagGeo::safeget(sp1, JAG_SP_START+5);
            double ny = JagGeo::safeget(sp1, JAG_SP_START+6);
			if ( colType1 == JAG_C_COL_TYPE_CONE ) {
				val = "CJAG=" + intToStr(srid) + "=0=CN=d 0:0:0:0:0:0 ";
			} else {
				val = "CJAG=" + intToStr(srid) + "=0=CL=d 0:0:0:0:0:0 ";
			}
			x = px + fx*(x-px); y = py + fy*(y-py); z = pz + fz*(z-pz);
			val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(r) + " " + d2s(h) + 
				+ " " + d2s(nx) + " " + d2s(ny);
		}
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING ) {
		JagLineString3D linestr;
		JagParser::addLineStringData( linestr, sp1 );
		linestr.scaleat( px,py,pz, fx,fy, 1.0, false );
		linestr.toJAG( JAG_C_COL_TYPE_LINESTRING, false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		JagLineString3D linestr;
		JagParser::addLineString3DData( linestr, sp1 );
		linestr.scaleat(px,py,pz, fx,fy,fz, true );
		linestr.toJAG( JAG_C_COL_TYPE_LINESTRING3D, true, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT ) {
		JagLineString3D linestr;
		JagParser::addLineStringData( linestr, sp1 );
		linestr.scaleat(px,py,pz, fx,fy, 1.0, false );
		linestr.toJAG( JAG_C_COL_TYPE_MULTIPOINT, false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		JagLineString3D linestr;
		JagParser::addLineString3DData( linestr, sp1 );
		linestr.scaleat(px, py, pz, fx,fy, fz, true );
		linestr.toJAG( JAG_C_COL_TYPE_MULTIPOINT3D, true, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		JagPolygon pgon;
		int rc = JagParser::addPolygonData( pgon, sp1, false );
		if ( rc < 0 ) return val;
		pgon.scaleat(px,py,pz, fx,fy, 1.0, false );
		pgon.toJAG( false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON3D ) {
		JagPolygon pgon;
		int rc = JagParser::addPolygon3DData( pgon, sp1, false );
		if ( rc < 0 ) return val;
		pgon.scaleat(px,py,pz, fx,fy,fz, true );
		pgon.toJAG( true, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		JagVector<JagPolygon> pgvec;
        int n = JagParser::addMultiPolygonData( pgvec, sp1, false, false );
        if ( n <= 0 ) return val;
		for ( int i=0; i < pgvec.size(); ++i ) { pgvec[i].scaleat(px,py,pz, fx,fy,1.0, false ); }
		bool rc = JagGeo::toJAG( pgvec, false, true, sp1[1], srid, val );
		if ( ! rc ) return val; 
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		JagVector<JagPolygon> pgvec;
        int n = JagParser::addMultiPolygonData( pgvec, sp1, false, true );
        if ( n <= 0 ) return val;
		for ( int i=0; i < pgvec.size(); ++i ) { pgvec[i].scaleat(px,py,pz, fx,fy,fz, true ); }
		bool rc = JagGeo::toJAG( pgvec, true, true, sp1[1], srid, val );
		if ( ! rc ) return val; 
	} else {
		return val;
	}

	return val;
}

bool BinaryOpNode::doAllReverse( int srid, const Jstr &colType1, const JagStrSplit &sp1, const Jstr &carg, Jstr &val )
{
	if ( sp1.size() < 1 ) return false;

	if ( colType1 == JAG_C_COL_TYPE_LINE  ) {
		val = sp1[0] + " " + sp1[1];
		val += Jstr(" ") + sp1[JAG_SP_START+2] + Jstr(" ") + sp1[JAG_SP_START+3] + Jstr(" ") 
			  + sp1[JAG_SP_START+0] + Jstr(" ") + sp1[JAG_SP_START+1];  
	} else if ( colType1 == JAG_C_COL_TYPE_LINE3D ) {
		val = sp1[0] + " " + sp1[1];
		val += Jstr(" ") + sp1[JAG_SP_START+3] + " " + sp1[JAG_SP_START+4] + " " + sp1[JAG_SP_START+5]
			  + sp1[JAG_SP_START+0] + Jstr(" ") + sp1[JAG_SP_START+1] + sp1[JAG_SP_START+2];  
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING ) {
		JagLineString linestr;
		JagParser::addLineStringData( linestr, sp1 );
		linestr.reverse();
		linestr.toJAG( JAG_C_COL_TYPE_LINESTRING, false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		JagLineString3D linestr;
		JagParser::addLineString3DData( linestr, sp1 );
		linestr.reverse();
		linestr.toJAG( JAG_C_COL_TYPE_LINESTRING3D, true, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		JagPolygon pgon;
		int rc = JagParser::addPolygonData( pgon, sp1, false );
		if ( rc < 0 ) return false;
		pgon.reverse();
		pgon.toJAG( false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON3D ) {
		JagPolygon pgon;
		int rc = JagParser::addPolygon3DData( pgon, sp1, false );
		if ( rc < 0 ) return false;
		pgon.reverse();
		pgon.toJAG( true, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		JagVector<JagPolygon> pgvec;
        int n = JagParser::addMultiPolygonData( pgvec, sp1, false, false );
        if ( n <= 0 ) return false;
		for ( int i=0; i < pgvec.size(); ++i ) { pgvec[i].reverse(); }
		bool rc = JagGeo::toJAG( pgvec, false, true, sp1[1], srid, val );
		if ( ! rc ) return false; 
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		JagVector<JagPolygon> pgvec;
        int n = JagParser::addMultiPolygonData( pgvec, sp1, false, true );
        if ( n <= 0 ) return false;
		for ( int i=0; i < pgvec.size(); ++i ) { pgvec[i].reverse(); }
		bool rc = JagGeo::toJAG( pgvec, true, true, sp1[1], srid, val );
		if ( ! rc ) return false; 
	} else {
		return false;
	}

	return true;
}

bool BinaryOpNode::doAllScale( int srid, const Jstr &colType1, const JagStrSplit &sp1, const Jstr &carg, Jstr &val )
{
	prt(("s1727 doAllScale sp1.size()=%d\n", sp1.size() ));
	if ( sp1.size() < 1 ) return false;
	double fx, fy, fz;
	fx = fy = fz = 1.0;
	JagStrSplit sf( carg, ':', true);
	if ( 3 == sf.size() ) {
		fx = fabs(sf[0].tof()); fy = fabs(sf[1].tof()); fz = fabs(sf[2].tof());
	} else if ( 2 == sf.size() ) {
		fx = fabs(sf[0].tof()); fy = fabs(sf[1].tof()); 
	} else {
		fx = fy = fz = fabs(carg.tof());
	}
	prt(("s1827 doAllScale fx=%.2f fy=%.2f fz=%.2f colType1=%s\n", fx, fy, fz, colType1.c_str() ));

	if ( colType1 == JAG_C_COL_TYPE_POINT ) {
		val = "CJAG=" + intToStr(srid) + "=0=PT=d 0:0:0:0 ";
		val += d2s(sp1[JAG_SP_START+0].tof()*fx) + Jstr(" ") + d2s(sp1[JAG_SP_START+1].tof()*fy);
	} else if ( colType1 == JAG_C_COL_TYPE_POINT3D ) {
		val = "CJAG=" + intToStr(srid) + "=0=PT=d 0:0:0:0 ";
		val += d2s(sp1[JAG_SP_START+0].tof()*fx) + Jstr(" ") + d2s(sp1[JAG_SP_START+1].tof()*fy)
			   + Jstr(" ") + d2s(sp1[JAG_SP_START+2].tof()*fz);
	} else if ( colType1 == JAG_C_COL_TYPE_LINE  ) {
		val = "CJAG=" + intToStr(srid) + "=0=LN=d 0:0:0:0 ";
		val += d2s(sp1[JAG_SP_START+0].tof()*fx) + Jstr(" ") + d2s(sp1[JAG_SP_START+1].tof()*fy) + Jstr(" ") 
			  + d2s(sp1[JAG_SP_START+2].tof()*fx) + Jstr(" ") + d2s(sp1[JAG_SP_START+3].tof()*fy);  
	} else if ( colType1 == JAG_C_COL_TYPE_LINE3D ) {
		val = "CJAG=" + intToStr(srid) + "=0=LN3=d 0:0:0:0:0:0 ";
		val += d2s(sp1[JAG_SP_START+0].tof()*fx) + " " + d2s(sp1[JAG_SP_START+1].tof()*fy) 
				+ " " + d2s(sp1[JAG_SP_START+2].tof()*fz) + d2s(sp1[JAG_SP_START+3].tof()*fx) + Jstr(" ") 
			  + d2s(sp1[JAG_SP_START+4].tof()*fy) + d2s(sp1[JAG_SP_START+5].tof()*fz);  
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE  ) {
		val = "CJAG=" + intToStr(srid) + "=0=TR=d 0:0:0:0 ";
		val += d2s(sp1[JAG_SP_START+0].tof()*fx) + Jstr(" ") + d2s(sp1[JAG_SP_START+1].tof()*fy) 
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+2].tof()*fx) 
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+3].tof()*fy)
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+4].tof()*fx)
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+5].tof()*fy);
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE3D  ) {
		val = "CJAG=" + intToStr(srid) + "=0=TR3=d 0:0:0:0:0:0 ";
		val += d2s(sp1[JAG_SP_START+0].tof()*fx) + Jstr(" ") + d2s(sp1[JAG_SP_START+1].tof()*fy) 
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+2].tof()*fz) 
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+3].tof()*fx) + Jstr(" ") + d2s(sp1[JAG_SP_START+4].tof()*fy)
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+5].tof()*fz) + Jstr(" ") + d2s(sp1[JAG_SP_START+6].tof()*fx)
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+7].tof()*fy) + Jstr(" ") + d2s(sp1[JAG_SP_START+8].tof()*fz);
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )*fx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )*fy;
		double r = jagatof( sp1[JAG_SP_START+2].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+3);
		double a = r *fx;
		double b = r *fy;
		val = "CJAG=" + intToStr(srid) + "=0=RC=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx);
		prt(("s6262 JAG_C_COL_TYPE_SQUARE val=%s\n", val.c_str() ));
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE3D ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )*fx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )*fy;
		double z = jagatof( sp1[JAG_SP_START+2].c_str() )*fz;
		double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+4);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+5);
		double a = r *fx;
		double b = r *fy;
		val = "CJAG=" + intToStr(srid) + "=0=RC3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )*fx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )*fy;
		double a = jagatof( sp1[JAG_SP_START+2].c_str() )*fx;
		double b = jagatof( sp1[JAG_SP_START+3].c_str() )*fy;
		double nx = JagGeo::safeget(sp1, JAG_SP_START+3);
		val = "CJAG=" + intToStr(srid) + "=0=RC=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE3D ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )*fx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )*fy;
		double z = jagatof( sp1[JAG_SP_START+2].c_str() )*fz;
		double a = jagatof( sp1[JAG_SP_START+3].c_str() )*fx;
		double b = jagatof( sp1[JAG_SP_START+4].c_str() )*fy;
		double nx = JagGeo::safeget(sp1, JAG_SP_START+5);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+6);
		val = "CJAG=" + intToStr(srid) + "=0=RC3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )*fx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )*fy;
		double r = jagatof( sp1[JAG_SP_START+2].c_str() );
		double a = r *fx;
		double b = r *fy;
		val = "CJAG=" + intToStr(srid) + "=0=EL=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " 0.0";
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE3D ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )*fx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )*fy;
		double z = jagatof( sp1[JAG_SP_START+2].c_str() )*fz;
		double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+4);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+5);
		double a = r *fx;
		double b = r *fy;
		val = "CJAG=" + intToStr(srid) + "=0=EL3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx) + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )*fx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )*fy;
		double a = jagatof( sp1[JAG_SP_START+2].c_str() )*fx;
		double b = jagatof( sp1[JAG_SP_START+3].c_str() )*fy;
		double nx = JagGeo::safeget(sp1, JAG_SP_START+3);
		val = "CJAG=" + intToStr(srid) + "=0=EL=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE3D ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )*fx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )*fy;
		double z = jagatof( sp1[JAG_SP_START+2].c_str() )*fz;
		double a = jagatof( sp1[JAG_SP_START+3].c_str() )*fx;
		double b = jagatof( sp1[JAG_SP_START+4].c_str() )*fy;
		double nx = JagGeo::safeget(sp1, JAG_SP_START+5);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+6);
		val = "CJAG=" + intToStr(srid) + "=0=EL3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_CUBE  ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )*fx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )*fy;
		double z = jagatof( sp1[JAG_SP_START+2].c_str() )*fz;
		double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = jagatof( sp1[JAG_SP_START+4].c_str() );
		double ny = jagatof( sp1[JAG_SP_START+5].c_str() );
		double a = r *fx;
		double b = r *fy;
		double c = r *fz;
		val = "CJAG=" + intToStr(srid) + "=0=BX=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(c) 
				+ " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )*fx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )*fy;
		double z = jagatof( sp1[JAG_SP_START+2].c_str() )*fz;
		double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double a = r *fx;
		double b = r *fy;
		double c = r *fz;
		val = "CJAG=" + intToStr(srid) + "=0=ES=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(c) + " 0.0 0.0";
	} else if ( colType1 == JAG_C_COL_TYPE_BOX || colType1 == JAG_C_COL_TYPE_ELLIPSOID  ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )*fx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )*fy;
		double z = jagatof( sp1[JAG_SP_START+2].c_str() )*fz;
		double a = jagatof( sp1[JAG_SP_START+3].c_str() )*fx;
		double b = jagatof( sp1[JAG_SP_START+4].c_str() )*fy;
		double c = jagatof( sp1[JAG_SP_START+5].c_str() )*fz;
		double nx = jagatof( sp1[JAG_SP_START+6].c_str() );
		double ny = jagatof( sp1[JAG_SP_START+7].c_str() );
		if ( colType1 == JAG_C_COL_TYPE_BOX ) {
			val = "CJAG=" + intToStr(srid) + "=0=BX=d 0:0:0:0:0:0 ";
		} else {
			val = "CJAG=" + intToStr(srid) + "=0=ES=d 0:0:0:0:0:0 ";
		}
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(c) 
				+ " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_CONE || colType1 == JAG_C_COL_TYPE_CYLINDER  ) {
		if ( JagGeo::jagEQ(fx,fy) && JagGeo::jagEQ(fx,fz) ) {
            double x = jagatof( sp1[JAG_SP_START+0].c_str() )*fx;
            double y = jagatof( sp1[JAG_SP_START+1].c_str() )*fx;
            double z = jagatof( sp1[JAG_SP_START+2].c_str() )*fx;
            double r = jagatof( sp1[JAG_SP_START+3].c_str() )*fx;
            double h = jagatof( sp1[JAG_SP_START+4].c_str() )*fx;
            double nx = JagGeo::safeget(sp1, JAG_SP_START+5);
            double ny = JagGeo::safeget(sp1, JAG_SP_START+6);
			if ( colType1 == JAG_C_COL_TYPE_CONE ) {
				val = "CJAG=" + intToStr(srid) + "=0=CN=d 0:0:0:0:0:0 ";
			} else {
				val = "CJAG=" + intToStr(srid) + "=0=CL=d 0:0:0:0:0:0 ";
			}
			val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(r) + " " + d2s(h) + 
				+ " " + d2s(nx) + " " + d2s(ny);
		}
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING ) {
		JagLineString3D linestr;
		JagParser::addLineStringData( linestr, sp1 );
		linestr.scale( fx,fy, 1.0, false );
		linestr.toJAG( JAG_C_COL_TYPE_LINESTRING, false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		JagLineString3D linestr;
		JagParser::addLineString3DData( linestr, sp1 );
		linestr.scale( fx,fy,fz, true );
		linestr.toJAG( JAG_C_COL_TYPE_LINESTRING3D, true, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT ) {
		JagLineString3D linestr;
		JagParser::addLineStringData( linestr, sp1 );
		linestr.scale( fx,fy, 1.0, false );
		linestr.toJAG( JAG_C_COL_TYPE_MULTIPOINT, false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		JagLineString3D linestr;
		JagParser::addLineString3DData( linestr, sp1 );
		linestr.scale( fx,fy, fz, true );
		linestr.toJAG( JAG_C_COL_TYPE_MULTIPOINT3D, true, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		JagPolygon pgon;
		int rc = JagParser::addPolygonData( pgon, sp1, false );
		if ( rc < 0 ) return false;
		pgon.scale( fx,fy, 1.0, false );
		pgon.toJAG( false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON3D ) {
		JagPolygon pgon;
		int rc = JagParser::addPolygon3DData( pgon, sp1, false );
		if ( rc < 0 ) return false;
		pgon.scale( fx,fy,fz, true );
		pgon.toJAG( true, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		JagVector<JagPolygon> pgvec;
        int n = JagParser::addMultiPolygonData( pgvec, sp1, false, false );
        if ( n <= 0 ) return false;
		for ( int i=0; i < pgvec.size(); ++i ) { pgvec[i].scale( fx,fy,1.0, false ); }
		bool rc = JagGeo::toJAG( pgvec, false, true, sp1[1], srid, val );
		if ( ! rc ) return false; 
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		JagVector<JagPolygon> pgvec;
        int n = JagParser::addMultiPolygonData( pgvec, sp1, false, true );
        if ( n <= 0 ) return false;
		for ( int i=0; i < pgvec.size(); ++i ) { pgvec[i].scale( fx,fy,fz, true ); }
		bool rc = JagGeo::toJAG( pgvec, true, true, sp1[1], srid, val );
		if ( ! rc ) return false; 
	} else {
		return false;
	}

	return true;
}


bool BinaryOpNode::doAllScaleSize( int srid, const Jstr &colType1, const JagStrSplit &sp1, const Jstr &carg, Jstr &val )
{
	prt(("s1727 doAllScale sp1.size()=%d\n", sp1.size() ));
	if ( sp1.size() < 1 ) return false;
	double fx, fy, fz;
	fx = fy = fz = 1.0;
	JagStrSplit sf( carg, ':', true);
	if ( 3 == sf.size() ) {
		fx = fabs(sf[0].tof()); fy = fabs(sf[1].tof()); fz = fabs(sf[2].tof());
	} else if ( 2 == sf.size() ) {
		fx = fabs(sf[0].tof()); fy = fabs(sf[1].tof()); 
	} else {
		fx = fy = fz = fabs(carg.tof());
	}
	prt(("s1827 doAllScale fx=%.2f fy=%.2f fz=%.2f colType1=%s\n", fx, fy, fz, colType1.c_str() ));

	if ( colType1 == JAG_C_COL_TYPE_POINT  ) {
		val = sp1[0] + " " + sp1[1] + " " +  sp1[JAG_SP_START+0] + Jstr(" ") + sp1[JAG_SP_START+1];
	} else if ( colType1 == JAG_C_COL_TYPE_POINT3D  ) {
		val = sp1[0] + " " + sp1[1] + " " +  sp1[JAG_SP_START+0] + Jstr(" ") + sp1[JAG_SP_START+1] 
		      + " " + sp1[JAG_SP_START+2];
	} else if ( colType1 == JAG_C_COL_TYPE_LINE  ) {
		val = "CJAG=" + intToStr(srid) + "=0=LN=d 0:0:0:0 ";
		val += d2s(sp1[JAG_SP_START+0].tof()*fx) + Jstr(" ") + d2s(sp1[JAG_SP_START+1].tof()*fy) + Jstr(" ") 
			  + d2s(sp1[JAG_SP_START+2].tof()*fx) + Jstr(" ") + d2s(sp1[JAG_SP_START+3].tof()*fy);  
	} else if ( colType1 == JAG_C_COL_TYPE_LINE3D ) {
		val = "CJAG=" + intToStr(srid) + "=0=LN3=d 0:0:0:0:0:0 ";
		val += d2s(sp1[JAG_SP_START+0].tof()*fx) + " " + d2s(sp1[JAG_SP_START+1].tof()*fy) 
				+ " " + d2s(sp1[JAG_SP_START+2].tof()*fz) + d2s(sp1[JAG_SP_START+3].tof()*fx) + Jstr(" ") 
			  + d2s(sp1[JAG_SP_START+4].tof()*fy) + d2s(sp1[JAG_SP_START+5].tof()*fz);  
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE  ) {
		val = "CJAG=" + intToStr(srid) + "=0=TR=d 0:0:0:0 ";
		val += d2s(sp1[JAG_SP_START+0].tof()*fx) + Jstr(" ") + d2s(sp1[JAG_SP_START+1].tof()*fy) 
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+2].tof()*fx) 
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+3].tof()*fy)
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+4].tof()*fx)
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+5].tof()*fy);
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE3D  ) {
		val = "CJAG=" + intToStr(srid) + "=0=TR3=d 0:0:0:0:0:0 ";
		val += d2s(sp1[JAG_SP_START+0].tof()*fx) + Jstr(" ") + d2s(sp1[JAG_SP_START+1].tof()*fy) 
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+2].tof()*fz) 
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+3].tof()*fx) + Jstr(" ") + d2s(sp1[JAG_SP_START+4].tof()*fy)
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+5].tof()*fz) + Jstr(" ") + d2s(sp1[JAG_SP_START+6].tof()*fx)
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+7].tof()*fy) + Jstr(" ") + d2s(sp1[JAG_SP_START+8].tof()*fz);
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double r = jagatof( sp1[JAG_SP_START+2].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+3);
		double a = r *fx;
		double b = r *fy;
		val = "CJAG=" + intToStr(srid) + "=0=RC=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx);
		prt(("s6262 JAG_C_COL_TYPE_SQUARE val=%s\n", val.c_str() ));
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE3D ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double z = jagatof( sp1[JAG_SP_START+2].c_str() );
		double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+4);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+5);
		double a = r *fx;
		double b = r *fy;
		val = "CJAG=" + intToStr(srid) + "=0=RC3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double a = jagatof( sp1[JAG_SP_START+2].c_str() )*fx;
		double b = jagatof( sp1[JAG_SP_START+3].c_str() )*fy;
		double nx = JagGeo::safeget(sp1, JAG_SP_START+3);
		val = "CJAG=" + intToStr(srid) + "=0=RC=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE3D ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double z = jagatof( sp1[JAG_SP_START+2].c_str() );
		double a = jagatof( sp1[JAG_SP_START+3].c_str() )*fx;
		double b = jagatof( sp1[JAG_SP_START+4].c_str() )*fy;
		double nx = JagGeo::safeget(sp1, JAG_SP_START+5);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+6);
		val = "CJAG=" + intToStr(srid) + "=0=RC3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double r = jagatof( sp1[JAG_SP_START+2].c_str() );
		double a = r *fx;
		double b = r *fy;
		val = "CJAG=" + intToStr(srid) + "=0=EL=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " 0.0";
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE3D ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double z = jagatof( sp1[JAG_SP_START+2].c_str() );
		double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+4);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+5);
		double a = r *fx;
		double b = r *fy;
		val = "CJAG=" + intToStr(srid) + "=0=EL3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx) + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double a = jagatof( sp1[JAG_SP_START+2].c_str() )*fx;
		double b = jagatof( sp1[JAG_SP_START+3].c_str() )*fy;
		double nx = JagGeo::safeget(sp1, JAG_SP_START+3);
		val = "CJAG=" + intToStr(srid) + "=0=EL=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE3D ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double z = jagatof( sp1[JAG_SP_START+2].c_str() );
		double a = jagatof( sp1[JAG_SP_START+3].c_str() )*fx;
		double b = jagatof( sp1[JAG_SP_START+4].c_str() )*fy;
		double nx = JagGeo::safeget(sp1, JAG_SP_START+5);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+6);
		val = "CJAG=" + intToStr(srid) + "=0=EL3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_CUBE  ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double z = jagatof( sp1[JAG_SP_START+2].c_str() );
		double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = jagatof( sp1[JAG_SP_START+4].c_str() );
		double ny = jagatof( sp1[JAG_SP_START+5].c_str() );
		double a = r *fx;
		double b = r *fy;
		double c = r *fz;
		val = "CJAG=" + intToStr(srid) + "=0=BX=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(c) 
				+ " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double z = jagatof( sp1[JAG_SP_START+2].c_str() );
		double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double a = r *fx;
		double b = r *fy;
		double c = r *fz;
		val = "CJAG=" + intToStr(srid) + "=0=ES=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(c) + " 0.0 0.0";
	} else if ( colType1 == JAG_C_COL_TYPE_BOX || colType1 == JAG_C_COL_TYPE_ELLIPSOID  ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double z = jagatof( sp1[JAG_SP_START+2].c_str() );
		double a = jagatof( sp1[JAG_SP_START+3].c_str() )*fx;
		double b = jagatof( sp1[JAG_SP_START+4].c_str() )*fy;
		double c = jagatof( sp1[JAG_SP_START+5].c_str() )*fz;
		double nx = jagatof( sp1[JAG_SP_START+6].c_str() );
		double ny = jagatof( sp1[JAG_SP_START+7].c_str() );
		if ( colType1 == JAG_C_COL_TYPE_BOX ) {
			val = "CJAG=" + intToStr(srid) + "=0=BX=d 0:0:0:0:0:0 ";
		} else {
			val = "CJAG=" + intToStr(srid) + "=0=ES=d 0:0:0:0:0:0 ";
		}
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(c) 
				+ " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_CONE || colType1 == JAG_C_COL_TYPE_CYLINDER  ) {
		if ( JagGeo::jagEQ(fx,fy) && JagGeo::jagEQ(fx,fz) ) {
            double x = jagatof( sp1[JAG_SP_START+0].c_str() );
            double y = jagatof( sp1[JAG_SP_START+1].c_str() );
            double z = jagatof( sp1[JAG_SP_START+2].c_str() );
            double r = jagatof( sp1[JAG_SP_START+3].c_str() )*fx;
            double h = jagatof( sp1[JAG_SP_START+4].c_str() )*fx;
            double nx = JagGeo::safeget(sp1, JAG_SP_START+5);
            double ny = JagGeo::safeget(sp1, JAG_SP_START+6);
			if ( colType1 == JAG_C_COL_TYPE_CONE ) {
				val = "CJAG=" + intToStr(srid) + "=0=CN=d 0:0:0:0:0:0 ";
			} else {
				val = "CJAG=" + intToStr(srid) + "=0=CL=d 0:0:0:0:0:0 ";
			}
			val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(r) + " " + d2s(h) + 
				+ " " + d2s(nx) + " " + d2s(ny);
		}
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING ) {
		JagLineString3D linestr;
		JagParser::addLineStringData( linestr, sp1 );
		linestr.scale( fx,fy, 1.0, false );
		linestr.toJAG( JAG_C_COL_TYPE_LINESTRING, false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		JagLineString3D linestr;
		JagParser::addLineString3DData( linestr, sp1 );
		linestr.scale( fx,fy,fz, true );
		linestr.toJAG( JAG_C_COL_TYPE_LINESTRING3D, true, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT ) {
		JagLineString3D linestr;
		JagParser::addLineStringData( linestr, sp1 );
		linestr.scale( fx,fy, 1.0, false );
		linestr.toJAG( JAG_C_COL_TYPE_MULTIPOINT, false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		JagLineString3D linestr;
		JagParser::addLineString3DData( linestr, sp1 );
		linestr.scale( fx,fy, fz, true );
		linestr.toJAG( JAG_C_COL_TYPE_MULTIPOINT3D, true, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		JagPolygon pgon;
		int rc = JagParser::addPolygonData( pgon, sp1, false );
		if ( rc < 0 ) return false;
		pgon.scale( fx,fy, 1.0, false );
		pgon.toJAG( false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON3D ) {
		JagPolygon pgon;
		int rc = JagParser::addPolygon3DData( pgon, sp1, false );
		if ( rc < 0 ) return false;
		pgon.scale( fx,fy,fz, true );
		pgon.toJAG( true, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		JagVector<JagPolygon> pgvec;
        int n = JagParser::addMultiPolygonData( pgvec, sp1, false, false );
        if ( n <= 0 ) return false;
		for ( int i=0; i < pgvec.size(); ++i ) { pgvec[i].scale( fx,fy,1.0, false ); }
		bool rc = JagGeo::toJAG( pgvec, false, true, sp1[1], srid, val );
		if ( ! rc ) return false; 
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		JagVector<JagPolygon> pgvec;
        int n = JagParser::addMultiPolygonData( pgvec, sp1, false, true );
        if ( n <= 0 ) return false;
		for ( int i=0; i < pgvec.size(); ++i ) { pgvec[i].scale( fx,fy,fz, true ); }
		bool rc = JagGeo::toJAG( pgvec, true, true, sp1[1], srid, val );
		if ( ! rc ) return false; 
	} else {
		return false;
	}

	return true;
}

bool BinaryOpNode::doAllTranslate( int srid, const Jstr &colType1, const JagStrSplit &sp1, const Jstr &carg, Jstr &val )
{
	prt(("s1727 doAllTranslate sp1.size()=%d\n", sp1.size() ));
	if ( sp1.size() < 1 ) return false;
	double dx, dy, dz;
	dx = dy = dz = 0.0;
	JagStrSplit sf( carg, ':', true);
	if ( 3 == sf.size() ) {
		dx = fabs(sf[0].tof()); dy = fabs(sf[1].tof()); dz = fabs(sf[2].tof());
	} else if ( 2 == sf.size() ) {
		dx = fabs(sf[0].tof()); dy = fabs(sf[1].tof()); 
	} 
	prt(("s1827 doAllTranslate dx=%.2f dy=%.2f dz=%.2f colType1=%s\n", dx, dy, dz, colType1.c_str() ));
	double x, y, z;

	if ( colType1 == JAG_C_COL_TYPE_POINT  ) {
		val = "CJAG=" + intToStr(srid) + "=0=PT=d 0:0:0:0";
		x = sp1[JAG_SP_START+0].tof() + dx;
		y = sp1[JAG_SP_START+1].tof() + dy;
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);
	} else if ( colType1 == JAG_C_COL_TYPE_POINT3D  ) {
		val = "CJAG=" + intToStr(srid) + "=0=PT3=d 0:0:0:0:0:0";
		x = sp1[JAG_SP_START+0].tof() + dx;
		y = sp1[JAG_SP_START+1].tof() + dy;
		z = sp1[JAG_SP_START+2].tof() + dz;
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y) + " " + d2s(z);
	} else if ( colType1 == JAG_C_COL_TYPE_LINE  ) {
		val = "CJAG=" + intToStr(srid) + "=0=LN=d 0:0:0:0 ";
		val += d2s(sp1[JAG_SP_START+0].tof()+dx) + Jstr(" ") + d2s(sp1[JAG_SP_START+1].tof()+dy) + Jstr(" ") 
			  + d2s(sp1[JAG_SP_START+2].tof()+dx) + Jstr(" ") + d2s(sp1[JAG_SP_START+3].tof()+dy);  
	} else if ( colType1 == JAG_C_COL_TYPE_LINE3D ) {
		val = "CJAG=" + intToStr(srid) + "=0=LN3=d 0:0:0:0:0:0 ";
		val += d2s(sp1[JAG_SP_START+0].tof()+dx) + " " + d2s(sp1[JAG_SP_START+1].tof()+dy) 
				+ " " + d2s(sp1[JAG_SP_START+2].tof()+dz) + d2s(sp1[JAG_SP_START+3].tof()+dx) + Jstr(" ") 
			  + d2s(sp1[JAG_SP_START+4].tof()+dy) + d2s(sp1[JAG_SP_START+5].tof()+dz);  
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE  ) {
		val = "CJAG=" + intToStr(srid) + "=0=TR=d 0:0:0:0 ";
		val += d2s(sp1[JAG_SP_START+0].tof()+dx) + Jstr(" ") + d2s(sp1[JAG_SP_START+1].tof()+dy) 
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+2].tof()+dx) 
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+3].tof()+dy)
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+4].tof()+dx)
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+5].tof()+dy);
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE3D  ) {
		val = "CJAG=" + intToStr(srid) + "=0=TR3=d 0:0:0:0:0:0 ";
		val += d2s(sp1[JAG_SP_START+0].tof()+dx) + Jstr(" ") + d2s(sp1[JAG_SP_START+1].tof()+dy) 
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+2].tof()+dz) 
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+3].tof()+dx) + Jstr(" ") + d2s(sp1[JAG_SP_START+4].tof()+dy)
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+5].tof()+dz) + Jstr(" ") + d2s(sp1[JAG_SP_START+6].tof()+dx)
				+ Jstr(" ") + d2s(sp1[JAG_SP_START+7].tof()+dy) + Jstr(" ") + d2s(sp1[JAG_SP_START+8].tof()+dz);
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		double r = jagatof( sp1[JAG_SP_START+2].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+3);
		val = "CJAG=" + intToStr(srid) + "=0=SQ=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(r) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE3D ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		double z = jagatof( sp1[JAG_SP_START+2].c_str() )+dz;
		double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+4);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+5);
		val = "CJAG=" + intToStr(srid) + "=0=SQ3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(r) + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		double a = jagatof( sp1[JAG_SP_START+2].c_str() );
		double b = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+3);
		val = "CJAG=" + intToStr(srid) + "=0=RC=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE3D ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		double z = jagatof( sp1[JAG_SP_START+2].c_str() )+dz;
		double a = jagatof( sp1[JAG_SP_START+3].c_str() );
		double b = jagatof( sp1[JAG_SP_START+4].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+5);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+6);
		val = "CJAG=" + intToStr(srid) + "=0=RC3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		val = "CJAG=" + intToStr(srid) + "=0=CR=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + sp1[JAG_SP_START+2];
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE3D ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		double z = jagatof( sp1[JAG_SP_START+2].c_str() )+dz;
		//double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+4);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+5);
		val = "CJAG=" + intToStr(srid) + "=0=CR3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + sp1[JAG_SP_START+3]+ " " + d2s(nx) + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		double a = jagatof( sp1[JAG_SP_START+2].c_str() );
		double b = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+3);
		val = "CJAG=" + intToStr(srid) + "=0=EL=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE3D ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		double z = jagatof( sp1[JAG_SP_START+2].c_str() )+dz;
		double a = jagatof( sp1[JAG_SP_START+3].c_str() );
		double b = jagatof( sp1[JAG_SP_START+4].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+5);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+6);
		val = "CJAG=" + intToStr(srid) + "=0=EL3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_CUBE  ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		double z = jagatof( sp1[JAG_SP_START+2].c_str() )+dz;
		//double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = jagatof( sp1[JAG_SP_START+4].c_str() );
		double ny = jagatof( sp1[JAG_SP_START+5].c_str() );
		val = "CJAG=" + intToStr(srid) + "=0=CB=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + sp1[JAG_SP_START+3] + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		double z = jagatof( sp1[JAG_SP_START+2].c_str() )+dz;
		//double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		val = "CJAG=" + intToStr(srid) + "=0=SR=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + sp1[JAG_SP_START+3];
	} else if ( colType1 == JAG_C_COL_TYPE_BOX || colType1 == JAG_C_COL_TYPE_ELLIPSOID  ) {
		double x = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		double y = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		double z = jagatof( sp1[JAG_SP_START+2].c_str() )+dz;
		//double a = jagatof( sp1[JAG_SP_START+3].c_str() );
		//double b = jagatof( sp1[JAG_SP_START+4].c_str() );
		//double c = jagatof( sp1[JAG_SP_START+5].c_str() );
		double nx = jagatof( sp1[JAG_SP_START+6].c_str() );
		double ny = jagatof( sp1[JAG_SP_START+7].c_str() );
		if ( colType1 == JAG_C_COL_TYPE_BOX ) {
			val = "CJAG=" + intToStr(srid) + "=0=BX=d 0:0:0:0:0:0 ";
		} else {
			val = "CJAG=" + intToStr(srid) + "=0=ES=d 0:0:0:0:0:0 ";
		}
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) 
		     + " " + sp1[JAG_SP_START+3] + " " + sp1[JAG_SP_START+4] + " " + sp1[JAG_SP_START+5]
			 + " " + d2s(nx) + " " + d2s(ny);

	} else if ( colType1 == JAG_C_COL_TYPE_CONE || colType1 == JAG_C_COL_TYPE_CYLINDER  ) {
        double x = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
        double y = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
        double z = jagatof( sp1[JAG_SP_START+2].c_str() )+dz;
        //double r = jagatof( sp1[JAG_SP_START+3].c_str() );
        //double h = jagatof( sp1[JAG_SP_START+4].c_str() );
        double nx = JagGeo::safeget(sp1, JAG_SP_START+5);
        double ny = JagGeo::safeget(sp1, JAG_SP_START+6);
		if ( colType1 == JAG_C_COL_TYPE_CONE ) {
				val = "CJAG=" + intToStr(srid) + "=0=CN=d 0:0:0:0:0:0 ";
		} else {
				val = "CJAG=" + intToStr(srid) + "=0=CL=d 0:0:0:0:0:0 ";
		}
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + sp1[JAG_SP_START+3] + " " + sp1[JAG_SP_START+4] + 
				+ " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING ) {
		JagLineString3D linestr;
		JagParser::addLineStringData( linestr, sp1 );
		linestr.translate( dx,dy,dz, false );
		linestr.toJAG( JAG_C_COL_TYPE_LINESTRING, false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		JagLineString3D linestr;
		JagParser::addLineString3DData( linestr, sp1 );
		linestr.translate( dx,dy,dz, true );
		linestr.toJAG( JAG_C_COL_TYPE_LINESTRING3D, true, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT ) {
		JagLineString3D linestr;
		JagParser::addLineStringData( linestr, sp1 );
		linestr.translate( dx,dy,dz, false );
		linestr.toJAG( JAG_C_COL_TYPE_MULTIPOINT, false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		JagLineString3D linestr;
		JagParser::addLineString3DData( linestr, sp1 );
		linestr.translate( dx,dy,dz, true );
		linestr.toJAG( JAG_C_COL_TYPE_MULTIPOINT3D, true, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		JagPolygon pgon;
		int rc = JagParser::addPolygonData( pgon, sp1, false );
		if ( rc < 0 ) return false;
		pgon.translate( dx,dy,dz, false );
		pgon.toJAG( false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON3D ) {
		JagPolygon pgon;
		int rc = JagParser::addPolygon3DData( pgon, sp1, false );
		if ( rc < 0 ) return false;
		pgon.translate( dx,dy,dz, true );
		pgon.toJAG( true, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		JagVector<JagPolygon> pgvec;
        int n = JagParser::addMultiPolygonData( pgvec, sp1, false, false );
        if ( n <= 0 ) return false;
		for ( int i=0; i < pgvec.size(); ++i ) { pgvec[i].translate( dx,dy,dz, false ); }
		bool rc = JagGeo::toJAG( pgvec, false, true, sp1[1], srid, val );
		if ( ! rc ) return false; 
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		JagVector<JagPolygon> pgvec;
        int n = JagParser::addMultiPolygonData( pgvec, sp1, false, true );
        if ( n <= 0 ) return false;
		for ( int i=0; i < pgvec.size(); ++i ) { pgvec[i].translate( dx,dy,dz, true ); }
		bool rc = JagGeo::toJAG( pgvec, true, true, sp1[1], srid, val );
		if ( ! rc ) return false; 
	} else {
		return false;
	}

	return true;
}

bool BinaryOpNode::doAllAffine( int srid, const Jstr &colType1, const JagStrSplit &sp1, const Jstr &carg, Jstr &val )
{
	prt(("s1727 doAllAffine sp1.size()=%d carg=[%s]\n", sp1.size(), carg.c_str() ));
	if ( sp1.size() < 1 ) return false;
	double a,b,c,d,e,f,g,h,i, dx,dy,dz;
	JagStrSplit sf( carg, ':', true);
	if ( 6 == sf.size() ) {
		a = sf[0].tof(); b = sf[1].tof(); d = sf[2].tof(); e = sf[3].tof();
		dx = sf[4].tof(); dy = sf[5].tof();
	} else if ( 12 == sf.size() ) {
		a = sf[0].tof(); b = sf[1].tof(); c = sf[2].tof(); d = sf[3].tof();
		e = sf[4].tof(); f = sf[5].tof(); g = sf[6].tof(); h = sf[7].tof(); i = sf[8].tof();
		dx = sf[9].tof(); dy = sf[10].tof(); dz = sf[11].tof();
	} else return false;

	prt(("s1827 doAllTranslate dx=%.2f dy=%.2f dz=%.2f colType1=%s\n", dx, dy, dz, colType1.c_str() ));
	double x1, y1, z1, x, y, z;

	if ( colType1 == JAG_C_COL_TYPE_POINT  ) {
		val = "CJAG=" + intToStr(srid) + "=0=PT=d 0:0:0:0";
		x1 = sp1[JAG_SP_START+0].tof() ;
		y1 = sp1[JAG_SP_START+1].tof() ;
		affine2d(x1, y1, a, b, d, e, dx, dy, x, y );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);
	} else if ( colType1 == JAG_C_COL_TYPE_POINT3D  ) {
		val = "CJAG=" + intToStr(srid) + "=0=PT3=d 0:0:0:0:0:0";
		x1 = sp1[JAG_SP_START+0].tof() ;
		y1 = sp1[JAG_SP_START+1].tof() ;
		z1 = sp1[JAG_SP_START+2].tof() ;
		affine3d(x1, y1, z1, a, b, c, d, e, f, g, h, i, dx, dy, dz, x, y, z );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y) + " " + d2s(z);
	} else if ( colType1 == JAG_C_COL_TYPE_LINE  ) {
		val = "CJAG=" + intToStr(srid) + "=0=LN=d 0:0:0:0";
		x1 = sp1[JAG_SP_START+0].tof() ;
		y1 = sp1[JAG_SP_START+1].tof() ;
		affine2d(x1, y1, a, b, d, e, dx, dy, x, y );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);

		x1 = sp1[JAG_SP_START+2].tof() ;
		y1 = sp1[JAG_SP_START+3].tof() ;
		affine2d(x1, y1, a, b, d, e, dx, dy, x, y );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);
	} else if ( colType1 == JAG_C_COL_TYPE_LINE3D ) {
		val = "CJAG=" + intToStr(srid) + "=0=LN3=d 0:0:0:0:0:0 ";

		x1 = sp1[JAG_SP_START+0].tof() ;
		y1 = sp1[JAG_SP_START+1].tof() ;
		z1 = sp1[JAG_SP_START+2].tof() ;
		affine3d(x1, y1, z1, a, b, c, d, e, f, g, h, i, dx, dy, dz, x, y, z );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y) + " " + d2s(z);

		x1 = sp1[JAG_SP_START+3].tof() ;
		y1 = sp1[JAG_SP_START+4].tof() ;
		z1 = sp1[JAG_SP_START+5].tof() ;
		affine3d(x1, y1, z1, a, b, c, d, e, f, g, h, i, dx, dy, dz, x, y, z );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y) + " " + d2s(z);
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE  ) {
		val = "CJAG=" + intToStr(srid) + "=0=TR=d 0:0:0:0 ";

		x1 = sp1[JAG_SP_START+0].tof() ;
		y1 = sp1[JAG_SP_START+1].tof() ;
		affine2d(x1, y1, a, b, d, e, dx, dy, x, y );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);

		x1 = sp1[JAG_SP_START+2].tof() ;
		y1 = sp1[JAG_SP_START+3].tof() ;
		affine2d(x1, y1, a, b, d, e, dx, dy, x, y );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);

		x1 = sp1[JAG_SP_START+4].tof() ;
		y1 = sp1[JAG_SP_START+5].tof() ;
		affine2d(x1, y1, a, b, d, e, dx, dy, x, y );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);

	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE3D  ) {
		val = "CJAG=" + intToStr(srid) + "=0=TR3=d 0:0:0:0:0:0 ";

		x1 = sp1[JAG_SP_START+0].tof() ;
		y1 = sp1[JAG_SP_START+1].tof() ;
		z1 = sp1[JAG_SP_START+2].tof() ;
		affine3d(x1, y1, z1, a, b, c, d, e, f, g, h, i, dx, dy, dz, x, y, z );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y) + " " + d2s(z);

		x1 = sp1[JAG_SP_START+3].tof() ;
		y1 = sp1[JAG_SP_START+4].tof() ;
		z1 = sp1[JAG_SP_START+5].tof() ;
		affine3d(x1, y1, z1, a, b, c, d, e, f, g, h, i, dx, dy, dz, x, y, z );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y) + " " + d2s(z);

		x1 = sp1[JAG_SP_START+6].tof() ;
		y1 = sp1[JAG_SP_START+7].tof() ;
		z1 = sp1[JAG_SP_START+8].tof() ;
		affine3d(x1, y1, z1, a, b, c, d, e, f, g, h, i, dx, dy, dz, x, y, z );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y) + " " + d2s(z);

	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE ) {
		x1 = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		y1 = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		double r = jagatof( sp1[JAG_SP_START+2].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+3);
		affine2d(x1, y1, a, b, d, e, dx, dy, x, y );
		val = "CJAG=" + intToStr(srid) + "=0=SQ=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(r) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE3D ) {
		x1 = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		y1 = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		z1 = jagatof( sp1[JAG_SP_START+2].c_str() )+dz;
		double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+4);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+5);
		affine3d(x1, y1, z1, a, b, c, d, e, f, g, h, i, dx, dy, dz, x, y, z );
		val = "CJAG=" + intToStr(srid) + "=0=SQ3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(r) + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE ) {
		x1 = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		y1 = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		double a1 = jagatof( sp1[JAG_SP_START+2].c_str() );
		double b1 = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+3);
		affine2d(x1, y1, a, b, d, e, dx, dy, x, y );
		val = "CJAG=" + intToStr(srid) + "=0=RC=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a1) + " " + d2s(b1) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE3D ) {
		x1 = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		y1 = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		z1 = jagatof( sp1[JAG_SP_START+2].c_str() )+dz;
		double a1 = jagatof( sp1[JAG_SP_START+3].c_str() );
		double b1 = jagatof( sp1[JAG_SP_START+4].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+5);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+6);
		affine3d(x1, y1, z1, a, b, c, d, e, f, g, h, i, dx, dy, dz, x, y, z );
		val = "CJAG=" + intToStr(srid) + "=0=RC3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a1) + " " + d2s(b1) + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE ) {
		x1 = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		y1 = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		affine2d(x1, y1, a, b, d, e, dx, dy, x, y );
		val = "CJAG=" + intToStr(srid) + "=0=CR=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + sp1[JAG_SP_START+2];
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE3D ) {
		x1 = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		y1 = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		z1 = jagatof( sp1[JAG_SP_START+2].c_str() )+dz;
		//double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+4);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+5);
		affine3d(x1, y1, z1, a, b, c, d, e, f, g, h, i, dx, dy, dz, x, y, z );
		val = "CJAG=" + intToStr(srid) + "=0=CR3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + sp1[JAG_SP_START+3]+ " " + d2s(nx) + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE ) {
		x1 = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		y1 = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		double a1 = jagatof( sp1[JAG_SP_START+2].c_str() );
		double b1 = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+3);
		affine2d(x1, y1, a, b, d, e, dx, dy, x, y );
		val = "CJAG=" + intToStr(srid) + "=0=EL=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a1) + " " + d2s(b1) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE3D ) {
		x1 = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		y1 = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		z1 = jagatof( sp1[JAG_SP_START+2].c_str() )+dz;
		double a1 = jagatof( sp1[JAG_SP_START+3].c_str() );
		double b1 = jagatof( sp1[JAG_SP_START+4].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+5);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+6);
		affine3d(x1, y1, z1, a, b, c, d, e, f, g, h, i, dx, dy, dz, x, y, z );
		val = "CJAG=" + intToStr(srid) + "=0=EL3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a1) + " " + d2s(b1) + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_CUBE  ) {
		x1 = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		y1 = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		z1 = jagatof( sp1[JAG_SP_START+2].c_str() )+dz;
		//double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = jagatof( sp1[JAG_SP_START+4].c_str() );
		double ny = jagatof( sp1[JAG_SP_START+5].c_str() );
		affine3d(x1, y1, z1, a, b, c, d, e, f, g, h, i, dx, dy, dz, x, y, z );
		val = "CJAG=" + intToStr(srid) + "=0=CB=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + sp1[JAG_SP_START+3] + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_SPHERE ) {
		x1 = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		y1 = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		z1 = jagatof( sp1[JAG_SP_START+2].c_str() )+dz;
		//double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		affine3d(x1, y1, z1, a, b, c, d, e, f, g, h, i, dx, dy, dz, x, y, z );
		val = "CJAG=" + intToStr(srid) + "=0=SR=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + sp1[JAG_SP_START+3];
	} else if ( colType1 == JAG_C_COL_TYPE_BOX || colType1 == JAG_C_COL_TYPE_ELLIPSOID  ) {
		x1 = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
		y1 = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
		z1 = jagatof( sp1[JAG_SP_START+2].c_str() )+dz;
		//double a = jagatof( sp1[JAG_SP_START+3].c_str() );
		//double b = jagatof( sp1[JAG_SP_START+4].c_str() );
		//double c = jagatof( sp1[JAG_SP_START+5].c_str() );
		double nx = jagatof( sp1[JAG_SP_START+6].c_str() );
		double ny = jagatof( sp1[JAG_SP_START+7].c_str() );
		affine3d(x1, y1, z1, a, b, c, d, e, f, g, h, i, dx, dy, dz, x, y, z );
		if ( colType1 == JAG_C_COL_TYPE_BOX ) {
			val = "CJAG=" + intToStr(srid) + "=0=BX=d 0:0:0:0:0:0 ";
		} else {
			val = "CJAG=" + intToStr(srid) + "=0=ES=d 0:0:0:0:0:0 ";
		}
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) 
		     + " " + sp1[JAG_SP_START+3] + " " + sp1[JAG_SP_START+4] + " " + sp1[JAG_SP_START+5]
			 + " " + d2s(nx) + " " + d2s(ny);

	} else if ( colType1 == JAG_C_COL_TYPE_CONE || colType1 == JAG_C_COL_TYPE_CYLINDER  ) {
        x1 = jagatof( sp1[JAG_SP_START+0].c_str() )+dx;
        y1 = jagatof( sp1[JAG_SP_START+1].c_str() )+dy;
        z1 = jagatof( sp1[JAG_SP_START+2].c_str() )+dz;
        //double r = jagatof( sp1[JAG_SP_START+3].c_str() );
        //double h = jagatof( sp1[JAG_SP_START+4].c_str() );
        double nx = JagGeo::safeget(sp1, JAG_SP_START+5);
        double ny = JagGeo::safeget(sp1, JAG_SP_START+6);
		affine3d(x1, y1, z1, a, b, c, d, e, f, g, h, i, dx, dy, dz, x, y, z );
		if ( colType1 == JAG_C_COL_TYPE_CONE ) {
				val = "CJAG=" + intToStr(srid) + "=0=CN=d 0:0:0:0:0:0 ";
		} else {
				val = "CJAG=" + intToStr(srid) + "=0=CL=d 0:0:0:0:0:0 ";
		}
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + sp1[JAG_SP_START+3] + " " + sp1[JAG_SP_START+4] + 
				+ " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING ) {
		JagLineString3D linestr;
		JagParser::addLineStringData( linestr, sp1 );
		linestr.affine2d( a, b, d, e, dx,dy );
		linestr.toJAG( JAG_C_COL_TYPE_LINESTRING, false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		JagLineString3D linestr;
		JagParser::addLineString3DData( linestr, sp1 );
		linestr.affine3d(a, b, c, d, e, f, g, h, i, dx, dy, dz );
		linestr.toJAG( JAG_C_COL_TYPE_LINESTRING3D, true, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT ) {
		JagLineString3D linestr;
		JagParser::addLineStringData( linestr, sp1 );
		linestr.affine2d( a, b, d, e, dx,dy );
		linestr.toJAG( JAG_C_COL_TYPE_MULTIPOINT, false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		JagLineString3D linestr;
		JagParser::addLineString3DData( linestr, sp1 );
		linestr.affine3d(a, b, c, d, e, f, g, h, i, dx, dy, dz );
		linestr.toJAG( JAG_C_COL_TYPE_MULTIPOINT3D, true, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		JagPolygon pgon;
		int rc = JagParser::addPolygonData( pgon, sp1, false );
		if ( rc < 0 ) return false;
		pgon.affine2d( a, b, d, e, dx,dy );
		pgon.toJAG( false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON3D ) {
		JagPolygon pgon;
		int rc = JagParser::addPolygon3DData( pgon, sp1, false );
		if ( rc < 0 ) return false;
		pgon.affine3d(a, b, c, d, e, f, g, h, i, dx, dy, dz );
		pgon.toJAG( true, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		JagVector<JagPolygon> pgvec;
        int n = JagParser::addMultiPolygonData( pgvec, sp1, false, false );
        if ( n <= 0 ) return false;
		for ( int i=0; i < pgvec.size(); ++i ) { pgvec[i].affine2d( a, b, d, e, dx,dy ); }
		bool rc = JagGeo::toJAG( pgvec, false, true, sp1[1], srid, val );
		if ( ! rc ) return false; 
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		JagVector<JagPolygon> pgvec;
        int n = JagParser::addMultiPolygonData( pgvec, sp1, false, true );
        if ( n <= 0 ) return false;
		for ( int i=0; i < pgvec.size(); ++i ) { pgvec[i].affine3d(a, b, c, d, e, f, g, h, i, dx, dy, dz ); }
		bool rc = JagGeo::toJAG( pgvec, true, true, sp1[1], srid, val );
		if ( ! rc ) return false; 
	} else {
		return false;
	}

	return true;
}

bool BinaryOpNode::doAllRotateAt( int srid, const Jstr &colType1, const JagStrSplit &sp1, const Jstr &carg, Jstr &val )
{
	prt(("s1425 doAllRotateAt sp1.size()=%d\n", sp1.size() ));
	if ( sp1.size() < 1 ) return false;
	// rotate(geom, 10, 'degree' )
	// rotateat(geom, 10, 'degree', x0,y0 )
	double x0,y0,alpha;
	x0 = y0 = 0.0;
	JagStrSplit sf( carg, ':', true);
	char c;
	if ( 2 == sf.size() ) {
		alpha = sf[0].tof();
		c = *( sf[1].c_str() );
	} else if ( 4 == sf.size() ) {
		alpha = sf[0].tof();
		c = *( sf[1].c_str() );
		x0 = sf[2].tof();
		y0 = sf[3].tof();
	} else {
		return false;
	}

	if ( c == 'r' || c == 'R' ) {
		// radians
	} else if ( c == 'd' || c == 'D' ) {
		// degrees
		//alpha = alpha*180.0/JAG_PI;
		alpha = alpha * 0.01745329252;
	} else {
		return false;
	}

	double x1, y1, x, y, nx;

	if ( colType1 == JAG_C_COL_TYPE_POINT  ) {
		val = "CJAG=" + intToStr(srid) + "=0=PT=d 0:0:0:0";
		x1 = sp1[JAG_SP_START+0].tof();
		y1 = sp1[JAG_SP_START+1].tof();
		rotateat( x1, y1, alpha, x0, y0, x, y );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);
	} else if ( colType1 == JAG_C_COL_TYPE_LINE  ) {
		val = "CJAG=" + intToStr(srid) + "=0=LN=d 0:0:0:0";
		x1 = sp1[JAG_SP_START+0].tof();
		y1 = sp1[JAG_SP_START+1].tof();
		rotateat( x1, y1, alpha, x0, y0, x, y );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);

		x1 = sp1[JAG_SP_START+2].tof();
		y1 = sp1[JAG_SP_START+3].tof();
		rotateat( x1, y1, alpha, x0, y0, x, y );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE  ) {
		val = "CJAG=" + intToStr(srid) + "=0=TR=d 0:0:0:0";
		x1 = sp1[JAG_SP_START+0].tof();
		y1 = sp1[JAG_SP_START+1].tof();
		rotateat( x1, y1, alpha, x0, y0, x, y );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);

		x1 = sp1[JAG_SP_START+2].tof();
		y1 = sp1[JAG_SP_START+3].tof();
		rotateat( x1, y1, alpha, x0, y0, x, y );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);

		x1 = sp1[JAG_SP_START+4].tof();
		y1 = sp1[JAG_SP_START+5].tof();
		rotateat( x1, y1, alpha, x0, y0, x, y );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE ) {
		double x1 = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y1 = jagatof( sp1[JAG_SP_START+1].c_str() );
		double r = jagatof( sp1[JAG_SP_START+2].c_str() );
		double oldnx = JagGeo::safeget(sp1, JAG_SP_START+3);
		rotateat( x1, y1, alpha, x0, y0, x, y );
		rotatenx( oldnx, alpha, nx );
		val = "CJAG=" + intToStr(srid) + "=0=SQ=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(r) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x1 = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y1 = jagatof( sp1[JAG_SP_START+1].c_str() );
		double a = jagatof( sp1[JAG_SP_START+2].c_str() );
		double b = jagatof( sp1[JAG_SP_START+3].c_str() );
		double oldnx = JagGeo::safeget(sp1, JAG_SP_START+3);
		rotateat( x1, y1, alpha, x0, y0, x, y );
		rotatenx( oldnx, alpha, nx );
		val = "CJAG=" + intToStr(srid) + "=0=RC=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE ) {
		double x1 = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y1 = jagatof( sp1[JAG_SP_START+1].c_str() );
		val = "CJAG=" + intToStr(srid) + "=0=CR=d 0:0:0:0 ";
		rotateat( x1, y1, alpha, x0, y0, x, y );
		val += d2s(x) + " " + d2s(y) + " " + sp1[JAG_SP_START+2];
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x1 = jagatof( sp1[JAG_SP_START+0].c_str() );
		double y1 = jagatof( sp1[JAG_SP_START+1].c_str() );
		double a = jagatof( sp1[JAG_SP_START+2].c_str() );
		double b = jagatof( sp1[JAG_SP_START+3].c_str() );
		double oldnx = JagGeo::safeget(sp1, JAG_SP_START+3);
		val = "CJAG=" + intToStr(srid) + "=0=EL=d 0:0:0:0 ";
		rotateat( x1, y1, alpha, x0, y0, x, y );
		rotatenx( oldnx, alpha, nx );
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING ) {
		JagLineString3D linestr;
		JagParser::addLineStringData( linestr, sp1 );
		linestr.rotateat(  alpha, x0, y0 );
		linestr.toJAG( JAG_C_COL_TYPE_LINESTRING, false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT ) {
		JagLineString3D linestr;
		JagParser::addLineStringData( linestr, sp1 );
		linestr.rotateat(  alpha, x0, y0 );
		linestr.toJAG( JAG_C_COL_TYPE_MULTIPOINT, false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		JagPolygon pgon;
		int rc = JagParser::addPolygonData( pgon, sp1, false );
		if ( rc < 0 ) return false;
		pgon.rotateat(  alpha, x0, y0 );
		pgon.toJAG( false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		JagVector<JagPolygon> pgvec;
        int n = JagParser::addMultiPolygonData( pgvec, sp1, false, false );
        if ( n <= 0 ) return false;
		for ( int i=0; i < pgvec.size(); ++i ) { pgvec[i].rotateat( alpha, x0, y0 ); }
		bool rc = JagGeo::toJAG( pgvec, false, true, sp1[1], srid, val );
		if ( ! rc ) return false; 
	} else {
		return false;
	}

	return true;
}

bool BinaryOpNode::doAllRotateSelf( int srid, const Jstr &colType1, const JagStrSplit &sp1, const Jstr &carg, Jstr &val )
{
	prt(("s1426 doAllRotateSelf sp1.size()=%d\n", sp1.size() ));
	if ( sp1.size() < 1 ) return false;
	// rotateself(geom, 10, 'degree' )
	double alpha;
	JagStrSplit sf( carg, ':', true);
	char c;
	if ( 2 == sf.size() ) {
		alpha = sf[0].tof();
		c = *( sf[1].c_str() );
	} else {
		return false;
	}

	if ( c == 'r' || c == 'R' ) {
		// radians
	} else if ( c == 'd' || c == 'D' ) {
		// degrees
		//alpha = alpha*180.0/JAG_PI;
		alpha = alpha * 0.01745329252;
	} else {
		return false;
	}

	double x, y, x1, y1, nx, x0, y0;

	if ( colType1 == JAG_C_COL_TYPE_POINT  ) {
		val = "CJAG=" + intToStr(srid) + "=0=PT=d 0:0:0:0";
		x = sp1[JAG_SP_START+0].tof();
		y = sp1[JAG_SP_START+1].tof();
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);
	} else if ( colType1 == JAG_C_COL_TYPE_LINE  ) {
		val = "CJAG=" + intToStr(srid) + "=0=LN=d 0:0:0:0";
		x1 = sp1[JAG_SP_START+0].tof();
		y1 = sp1[JAG_SP_START+1].tof();

		double x2 = sp1[JAG_SP_START+2].tof();
		double y2 = sp1[JAG_SP_START+3].tof();

		rotateat( x1, y1, alpha, (x1+x2)/2.0, (y1+y2)/2.0, x, y );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);

		rotateat( x2, y2, alpha, (x1+x2)/2.0, (y1+y2)/2.0, x, y );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE  ) {
		val = "CJAG=" + intToStr(srid) + "=0=TR=d 0:0:0:0";
		x1 = sp1[JAG_SP_START+0].tof();
		y1 = sp1[JAG_SP_START+1].tof();
		double x2 = sp1[JAG_SP_START+2].tof();
		double y2 = sp1[JAG_SP_START+3].tof();
		double x3 = sp1[JAG_SP_START+4].tof();
		double y3 = sp1[JAG_SP_START+5].tof();

		x0 = (x1+x2+x3)/3.0;  // center
		y0 = (y1+y2+y3)/3.0;
		rotateat( x1, y1, alpha, x0, y0, x, y );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);
		rotateat( x2, y2, alpha, x0, y0, x, y );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);
		rotateat( x3, y3, alpha, x0, y0, x, y );
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE ) {
		x = jagatof( sp1[JAG_SP_START+0].c_str() );
		y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double r = jagatof( sp1[JAG_SP_START+2].c_str() );
		double oldnx = JagGeo::safeget(sp1, JAG_SP_START+3);
		rotatenx( oldnx, alpha, nx );
		val = "CJAG=" + intToStr(srid) + "=0=SQ=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(r) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE ) {
		x = jagatof( sp1[JAG_SP_START+0].c_str() );
		y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double a = jagatof( sp1[JAG_SP_START+2].c_str() );
		double b = jagatof( sp1[JAG_SP_START+3].c_str() );
		double oldnx = JagGeo::safeget(sp1, JAG_SP_START+3);
		rotatenx( oldnx, alpha, nx );
		val = "CJAG=" + intToStr(srid) + "=0=RC=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE ) {
		x = jagatof( sp1[JAG_SP_START+0].c_str() );
		y = jagatof( sp1[JAG_SP_START+1].c_str() );
		val = "CJAG=" + intToStr(srid) + "=0=CR=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + sp1[JAG_SP_START+2];
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE ) {
		x = jagatof( sp1[JAG_SP_START+0].c_str() );
		y = jagatof( sp1[JAG_SP_START+1].c_str() );
		double a = jagatof( sp1[JAG_SP_START+2].c_str() );
		double b = jagatof( sp1[JAG_SP_START+3].c_str() );
		double oldnx = JagGeo::safeget(sp1, JAG_SP_START+3);
		val = "CJAG=" + intToStr(srid) + "=0=EL=d 0:0:0:0 ";
		rotatenx( oldnx, alpha, nx );
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING ) {
		JagLineString3D linestr;
		JagParser::addLineStringData( linestr, sp1 );
		linestr.rotateself(  alpha );
		linestr.toJAG( JAG_C_COL_TYPE_LINESTRING, false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT ) {
		JagLineString3D linestr;
		JagParser::addLineStringData( linestr, sp1 );
		linestr.rotateself(  alpha );
		linestr.toJAG( JAG_C_COL_TYPE_MULTIPOINT, false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		JagPolygon pgon;
		int rc = JagParser::addPolygonData( pgon, sp1, false );
		if ( rc < 0 ) return false;
		pgon.rotateself(  alpha );
		pgon.toJAG( false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		JagVector<JagPolygon> pgvec;
        int n = JagParser::addMultiPolygonData( pgvec, sp1, false, false );
        if ( n <= 0 ) return false;
		for ( int i=0; i < pgvec.size(); ++i ) { pgvec[i].rotateself( alpha ); }
		bool rc = JagGeo::toJAG( pgvec, false, true, sp1[1], srid, val );
		if ( ! rc ) return false; 
	} else {
		return false;
	}

	return true;
}



bool BinaryOpNode::doAllTransScale( int srid, const Jstr &colType1, const JagStrSplit &sp1, const Jstr &carg, Jstr &val )
{
	prt(("s1727 doAllTransScale sp1.size()=%d carg=[%s]\n", sp1.size(), carg.c_str() ));
	if ( sp1.size() < 1 ) return false;
	double dx, dy, dz;
	double fx, fy, fz;
	dx = dy = dz = 0.0;
	fx = fy = fz = 1.0;
	JagStrSplit sf( carg, ':', true);
	prt(("s3828 sf.size=%d\n", sf.size() ));
	if ( 4 == sf.size() ) {
		dx = fabs(sf[0].tof()); dy = fabs(sf[1].tof()); 
		fx = fabs(sf[2].tof()); fy = fabs(sf[3].tof());
	} else if ( 6 == sf.size() ) {
		dx = fabs(sf[0].tof()); dy = fabs(sf[1].tof()); dz = fabs(sf[2].tof());
		fx = fabs(sf[3].tof()); fy = fabs(sf[4].tof()); fz = fabs(sf[5].tof());
	} else {
		return false;
	}
	prt(("s1827 doAllTransScale dx=%.2f dy=%.2f dz=%.2f colType1=%s\n", dx, dy, dz, colType1.c_str() ));
	prt(("s1827 doAllTransScale fx=%.2f fy=%.2f fz=%.2f colType1=%s\n", fx, fy, fz, colType1.c_str() ));
	double x, y, z;

	if ( colType1 == JAG_C_COL_TYPE_POINT  ) {
		val = "CJAG=" + intToStr(srid) + "=0=PT=d 0:0:0:0";
		x = (sp1[JAG_SP_START+0].tof() + dx)*fx;
		y = (sp1[JAG_SP_START+1].tof() + dy)*fy;
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y);
	} else if ( colType1 == JAG_C_COL_TYPE_POINT3D  ) {
		val = "CJAG=" + intToStr(srid) + "=0=PT3=d 0:0:0:0:0:0";
		x = (sp1[JAG_SP_START+0].tof() + dx)*fx;
		y = (sp1[JAG_SP_START+1].tof() + dy)*fy;
		z = (sp1[JAG_SP_START+2].tof() + dz)*fz;
		val += Jstr(" ") + d2s(x) + Jstr(" ") + d2s(y) + " " + d2s(z);
	} else if ( colType1 == JAG_C_COL_TYPE_LINE  ) {
		val = "CJAG=" + intToStr(srid) + "=0=LN=d 0:0:0:0 ";
		val += d2s((sp1[JAG_SP_START+0].tof()+dx)*fx) + Jstr(" ") + d2s((sp1[JAG_SP_START+1].tof()+dy)*fy) + Jstr(" ") 
			  + d2s((sp1[JAG_SP_START+2].tof()+dx)*fx) + Jstr(" ") + d2s((sp1[JAG_SP_START+3].tof()+dy)*fy);  
	} else if ( colType1 == JAG_C_COL_TYPE_LINE3D ) {
		val = "CJAG=" + intToStr(srid) + "=0=LN3=d 0:0:0:0:0:0 ";
		val += d2s((sp1[JAG_SP_START+0].tof()+dx)*fx) + " " + d2s((sp1[JAG_SP_START+1].tof()+dy)*fy) 
				+ " " + d2s((sp1[JAG_SP_START+2].tof()+dz)*fz) + d2s((sp1[JAG_SP_START+3].tof()+dx)*fx) + Jstr(" ") 
			  + d2s((sp1[JAG_SP_START+4].tof()+dy)*fy) + d2s((sp1[JAG_SP_START+5].tof()+dz)*fz);  
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE  ) {
		val = "CJAG=" + intToStr(srid) + "=0=TR=d 0:0:0:0 ";
		val += d2s((sp1[JAG_SP_START+0].tof()+dx)*fx) + Jstr(" ") + d2s((sp1[JAG_SP_START+1].tof()+dy)*fy) 
				+ Jstr(" ") + d2s((sp1[JAG_SP_START+2].tof()+dx)*fx) 
				+ Jstr(" ") + d2s((sp1[JAG_SP_START+3].tof()+dy)*fy)
				+ Jstr(" ") + d2s((sp1[JAG_SP_START+4].tof()+dx)*fx)
				+ Jstr(" ") + d2s((sp1[JAG_SP_START+5].tof()+dy)*fy);
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE3D  ) {
		val = "CJAG=" + intToStr(srid) + "=0=TR3=d 0:0:0:0:0:0 ";
		val += d2s((sp1[JAG_SP_START+0].tof()+dx)*fx) + Jstr(" ") + d2s((sp1[JAG_SP_START+1].tof()+dy)*fy) 
				+ Jstr(" ") + d2s((sp1[JAG_SP_START+2].tof()+dz)*fz) 
				+ Jstr(" ") + d2s((sp1[JAG_SP_START+3].tof()+dx)*fx) + Jstr(" ") + d2s((sp1[JAG_SP_START+4].tof()+dy)*fy)
				+ Jstr(" ") + d2s((sp1[JAG_SP_START+5].tof()+dz)*fz) + Jstr(" ") + d2s((sp1[JAG_SP_START+6].tof()+dx)*fx)
				+ Jstr(" ") + d2s((sp1[JAG_SP_START+7].tof()+dy)*fy) + Jstr(" ") + d2s((sp1[JAG_SP_START+8].tof()+dz)*fz);
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE ) {
		double x = (jagatof( sp1[JAG_SP_START+0].c_str() )+dx)*fx;
		double y = (jagatof( sp1[JAG_SP_START+1].c_str() )+dy)*fy;
		double r = jagatof( sp1[JAG_SP_START+2].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+3);
		double a = r*fx;
		double b = r*fy;
		val = "CJAG=" + intToStr(srid) + "=0=RC=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(a) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE3D ) {
		double x = (jagatof( sp1[JAG_SP_START+0].c_str() )+dx)*fx;
		double y = (jagatof( sp1[JAG_SP_START+1].c_str() )+dy)*fy;
		double z = (jagatof( sp1[JAG_SP_START+2].c_str() )+dz)*fz;
		double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+4);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+5);
		double a = r*fx;
		double b = r*fy;
		double c = r*fz;
		val = "CJAG=" + intToStr(srid) + "=0=RC3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(c) + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x = (jagatof( sp1[JAG_SP_START+0].c_str() )+dx)*fx;
		double y = (jagatof( sp1[JAG_SP_START+1].c_str() )+dy)*fy;
		double a = jagatof( sp1[JAG_SP_START+2].c_str() )*fx;
		double b = jagatof( sp1[JAG_SP_START+3].c_str() )*fy;
		double nx = JagGeo::safeget(sp1, JAG_SP_START+3);
		val = "CJAG=" + intToStr(srid) + "=0=RC=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE3D ) {
		double x = (jagatof( sp1[JAG_SP_START+0].c_str() )+dx)*fx;
		double y = (jagatof( sp1[JAG_SP_START+1].c_str() )+dy)*fy;
		double z = (jagatof( sp1[JAG_SP_START+2].c_str() )+dz)*fz;
		double a = jagatof( sp1[JAG_SP_START+3].c_str() )*fx;
		double b = jagatof( sp1[JAG_SP_START+4].c_str() )*fy;
		double nx = JagGeo::safeget(sp1, JAG_SP_START+5);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+6);
		val = "CJAG=" + intToStr(srid) + "=0=RC3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE ) {
		double x = (jagatof( sp1[JAG_SP_START+0].c_str() )+dx)*fx;
		double y = (jagatof( sp1[JAG_SP_START+1].c_str() )+dy)*fy;
		double r = jagatof( sp1[JAG_SP_START+2].c_str() );
		double a = r*fx;
		double b = r*fy;
		val = "CJAG=" + intToStr(srid) + "=0=EL=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " 0.0";
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE3D ) {
		double x = (jagatof( sp1[JAG_SP_START+0].c_str() )+dx)*fx;
		double y = (jagatof( sp1[JAG_SP_START+1].c_str() )+dy)*fy;
		double z = (jagatof( sp1[JAG_SP_START+2].c_str() )+dz)*fz;
		double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = JagGeo::safeget(sp1, JAG_SP_START+4);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+5);
		double a = r*fx;
		double b = r*fy;
		double c = r*fz;
		val = "CJAG=" + intToStr(srid) + "=0=EL3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " 
		     + d2s(a) + " " + d2s(b) + " " + d2s(c) + " " + d2s(nx) + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x = (jagatof( sp1[JAG_SP_START+0].c_str() )+dx)*fx;
		double y = (jagatof( sp1[JAG_SP_START+1].c_str() )+dy)*fy;
		double a = jagatof( sp1[JAG_SP_START+2].c_str() )*fx;
		double b = jagatof( sp1[JAG_SP_START+3].c_str() )*fy;
		double nx = JagGeo::safeget(sp1, JAG_SP_START+3);
		val = "CJAG=" + intToStr(srid) + "=0=EL=d 0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx);
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE3D ) {
		double x = (jagatof( sp1[JAG_SP_START+0].c_str() )+dx)*fx;
		double y = (jagatof( sp1[JAG_SP_START+1].c_str() )+dy)*fy;
		double z = (jagatof( sp1[JAG_SP_START+2].c_str() )+dz)*fz;
		double a = jagatof( sp1[JAG_SP_START+3].c_str() )*fx;
		double b = jagatof( sp1[JAG_SP_START+4].c_str() )*fy;
		double nx = JagGeo::safeget(sp1, JAG_SP_START+5);
		double ny = JagGeo::safeget(sp1, JAG_SP_START+6);
		val = "CJAG=" + intToStr(srid) + "=0=EL3=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(a) + " " + d2s(b) + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_CUBE  ) {
		double x = (jagatof( sp1[JAG_SP_START+0].c_str() )+dx)*fx;
		double y = (jagatof( sp1[JAG_SP_START+1].c_str() )+dy)*fy;
		double z = (jagatof( sp1[JAG_SP_START+2].c_str() )+dz)*fz;
		double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double nx = jagatof( sp1[JAG_SP_START+4].c_str() );
		double ny = jagatof( sp1[JAG_SP_START+5].c_str() );
		double a = r*fx;
		double b = r*fy;
		double c = r*fz;
		val = "CJAG=" + intToStr(srid) + "=0=BX=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " 
				+ d2s(a) + " " + d2s(b) + " " + d2s(c) + " " + d2s(nx) + " " + d2s(ny);
	} else if ( colType1 == JAG_C_COL_TYPE_SPHERE ) {
		double x = (jagatof( sp1[JAG_SP_START+0].c_str() )+dx)*fx;
		double y = (jagatof( sp1[JAG_SP_START+1].c_str() )+dy)*fy;
		double z = (jagatof( sp1[JAG_SP_START+2].c_str() )+dz)*fz;
		double r = jagatof( sp1[JAG_SP_START+3].c_str() );
		double a = r*fx;
		double b = r*fy;
		double c = r*fz;
		val = "CJAG=" + intToStr(srid) + "=0=ES=d 0:0:0:0:0:0 ";
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " 
				+ d2s(a) + " " + d2s(b) + " " + d2s(c) + " 0.0 0.0";
	} else if ( colType1 == JAG_C_COL_TYPE_BOX || colType1 == JAG_C_COL_TYPE_ELLIPSOID  ) {
		double x = (jagatof( sp1[JAG_SP_START+0].c_str() )+dx)*fx;
		double y = (jagatof( sp1[JAG_SP_START+1].c_str() )+dy)*fy;
		double z = (jagatof( sp1[JAG_SP_START+2].c_str() )+dz)*fz;
		double a = jagatof( sp1[JAG_SP_START+3].c_str() )*fx;
		double b = jagatof( sp1[JAG_SP_START+4].c_str() )*fy;
		double c = jagatof( sp1[JAG_SP_START+5].c_str() )*fz;
		double nx = jagatof( sp1[JAG_SP_START+6].c_str() );
		double ny = jagatof( sp1[JAG_SP_START+7].c_str() );
		if ( colType1 == JAG_C_COL_TYPE_BOX ) {
			val = "CJAG=" + intToStr(srid) + "=0=BX=d 0:0:0:0:0:0 ";
		} else {
			val = "CJAG=" + intToStr(srid) + "=0=ES=d 0:0:0:0:0:0 ";
		}
		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " "
			+ d2s(a) + " " + d2s(b) + " " + d2s(c) + " "
			 + d2s(nx) + " " + d2s(ny);

	} else if ( colType1 == JAG_C_COL_TYPE_CONE || colType1 == JAG_C_COL_TYPE_CYLINDER  ) {
		if ( JagGeo::jagEQ(fx,fy) && JagGeo::jagEQ(fx,fz) ) {
            double x = (jagatof( sp1[JAG_SP_START+0].c_str() )+dx)*fx;
            double y = (jagatof( sp1[JAG_SP_START+1].c_str() )+dy)*fx;
            double z = (jagatof( sp1[JAG_SP_START+2].c_str() )+dz)*fx;
            double r = jagatof( sp1[JAG_SP_START+3].c_str() )*fx;
            double h = jagatof( sp1[JAG_SP_START+4].c_str() )*fx;
            double nx = JagGeo::safeget(sp1, JAG_SP_START+5);
            double ny = JagGeo::safeget(sp1, JAG_SP_START+6);
    		if ( colType1 == JAG_C_COL_TYPE_CONE ) {
    				val = "CJAG=" + intToStr(srid) + "=0=CN=d 0:0:0:0:0:0 ";
    		} else {
    				val = "CJAG=" + intToStr(srid) + "=0=CL=d 0:0:0:0:0:0 ";
    		}
    		val += d2s(x) + " " + d2s(y) + " " + d2s(z) + " " + d2s(r) + " " + d2s(h) + 
    				+ " " + d2s(nx) + " " + d2s(ny);
		}
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING ) {
		JagLineString3D linestr;
		JagParser::addLineStringData( linestr, sp1 );
		linestr.transscale( dx,dy,dz, fx,fy,fz, false );
		linestr.toJAG( JAG_C_COL_TYPE_LINESTRING, false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		JagLineString3D linestr;
		JagParser::addLineString3DData( linestr, sp1 );
		linestr.transscale( dx,dy,dz, fx,fy,fz, true );
		linestr.toJAG( JAG_C_COL_TYPE_LINESTRING3D, true, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT ) {
		JagLineString3D linestr;
		JagParser::addLineStringData( linestr, sp1 );
		linestr.transscale( dx,dy,dz, fx,fy,fz, false );
		linestr.toJAG( JAG_C_COL_TYPE_MULTIPOINT, false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		JagLineString3D linestr;
		JagParser::addLineString3DData( linestr, sp1 );
		linestr.transscale( dx,dy,dz, fx,fy,fz, true );
		linestr.toJAG( JAG_C_COL_TYPE_MULTIPOINT3D, true, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		JagPolygon pgon;
		int rc = JagParser::addPolygonData( pgon, sp1, false );
		if ( rc < 0 ) return false;
		pgon.transscale( dx,dy,dz, fx,fy,fz, false );
		pgon.toJAG( false, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON3D ) {
		JagPolygon pgon;
		int rc = JagParser::addPolygon3DData( pgon, sp1, false );
		if ( rc < 0 ) return false;
		pgon.transscale( dx,dy,dz, fx,fy,fz, true );
		pgon.toJAG( true, true, sp1[1], srid, val );
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		JagVector<JagPolygon> pgvec;
        int n = JagParser::addMultiPolygonData( pgvec, sp1, false, false );
        if ( n <= 0 ) return false;
		for ( int i=0; i < pgvec.size(); ++i ) { pgvec[i].transscale( dx,dy,dz, fx,fy,fz, false ); }
		bool rc = JagGeo::toJAG( pgvec, false, true, sp1[1], srid, val );
		if ( ! rc ) return false; 
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		JagVector<JagPolygon> pgvec;
        int n = JagParser::addMultiPolygonData( pgvec, sp1, false, true );
        if ( n <= 0 ) return false;
		for ( int i=0; i < pgvec.size(); ++i ) { pgvec[i].transscale( dx,dy,dz, fx,fy,fz, true ); }
		bool rc = JagGeo::toJAG( pgvec, true, true, sp1[1], srid, val );
		if ( ! rc ) return false; 
	} else {
		return false;
	}

	return true;
}

bool BinaryOpNode::doAllIntersect( const Jstr& mark1, const Jstr &colType1, 
										int srid1, const JagStrSplit &sp1, const Jstr& mark2, 
										const Jstr &colType2, int srid2, const JagStrSplit &sp2, bool strict )
{
	//prt(("s2410 doAllIntersect colType1=[%s] colType2=[%s] \n", colType1.c_str(),  colType2.c_str() ));

	if ( colType1 == JAG_C_COL_TYPE_POINT ) {
		return JagGeo::doPointIntersect(  srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_POINT ) {
		return JagGeo::doPointIntersect(  srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_POINT3D ) {
		return JagGeo::doPoint3DIntersect(  srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_POINT3D ) {
		return JagGeo::doPoint3DIntersect(  srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE ) {
		return JagGeo::doCircleIntersect(  srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		return JagGeo::doCircleIntersect(  srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE3D ) {
		return JagGeo::doCircle3DIntersect(  srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE3D ) {
		return JagGeo::doCircle3DIntersect(  srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_SPHERE ) {
		return JagGeo::doSphereIntersect(  srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		return JagGeo::doSphereIntersect(  srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE ) {
		return JagGeo::doSquareIntersect(  srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		return JagGeo::doSquareIntersect(  srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE3D ) {
		return JagGeo::doSquare3DIntersect(  srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE3D ) {
		return JagGeo::doSquare3DIntersect(  srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_CUBE ) {
		return JagGeo::doCubeIntersect(  srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_CUBE ) {
		return JagGeo::doCubeIntersect(  srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE ) {
		return JagGeo::doRectangleIntersect(  srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		return JagGeo::doRectangleIntersect(  srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE3D ) {
		return JagGeo::doRectangle3DIntersect(  srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE3D ) {
		return JagGeo::doRectangle3DIntersect(  srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_BOX ) {
		return JagGeo::doBoxIntersect(  srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		return JagGeo::doBoxIntersect(  srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE ) {
		return JagGeo::doTriangleIntersect(  srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		return JagGeo::doTriangleIntersect(  srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE3D ) {
		return JagGeo::doTriangle3DIntersect(  srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_TRIANGLE3D ) {
		return JagGeo::doTriangle3DIntersect(  srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_CYLINDER ) {
		return JagGeo::doCylinderIntersect(  srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_CYLINDER ) {
		return JagGeo::doCylinderIntersect(  srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_CONE ) {
		return JagGeo::doConeIntersect(  srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		return JagGeo::doConeIntersect(  srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE ) {
		return JagGeo::doEllipseIntersect(  srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		return JagGeo::doEllipseIntersect(  srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSOID ) {
		return JagGeo::doEllipsoidIntersect(  srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		return JagGeo::doEllipsoidIntersect(  srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_LINE ) {
		return JagGeo::doLineIntersect(  srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_LINE ) {
		return JagGeo::doLineIntersect(  srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_LINE3D ) {
		return JagGeo::doLine3DIntersect(  srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_LINE3D ) {
		return JagGeo::doLine3DIntersect(  srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING ) {
		return JagGeo::doLineStringIntersect( mark1, srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING ) {
		return JagGeo::doLineStringIntersect( mark2, srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return JagGeo::doLineString3DIntersect( mark1, srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return JagGeo::doLineString3DIntersect( mark2, srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		return JagGeo::doPolygonIntersect( mark1, srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return JagGeo::doPolygonIntersect( mark2, srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON3D ) {
		return JagGeo::doPolygon3DIntersect( mark1, srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON3D ) {
		return JagGeo::doPolygon3DIntersect( mark2, srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING ) {
		return JagGeo::doPolygonIntersect( mark1, srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_MULTILINESTRING ) {
		return JagGeo::doPolygonIntersect( mark2, srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		return JagGeo::doPolygon3DIntersect( mark1, srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		return JagGeo::doPolygon3DIntersect( mark2, srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		return JagGeo::doMultiPolygonIntersect( mark1, srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		return JagGeo::doMultiPolygonIntersect( mark2, srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		return JagGeo::doMultiPolygon3DIntersect( mark1, srid1, sp1, mark2, colType2, srid2, sp2, strict);
	} else if ( colType2 == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		return JagGeo::doMultiPolygon3DIntersect( mark2, srid2, sp2, mark1, colType1, srid1, sp1, strict);
	} else if ( colType1 == JAG_C_COL_TYPE_RANGE
	             || colType1 == JAG_C_COL_TYPE_DOUBLE 
	             || colType1 == JAG_C_COL_TYPE_FLOAT
	             || colType1 == JAG_C_COL_TYPE_DATETIME
	             || colType1 == JAG_C_COL_TYPE_TIME
	             || colType1 == JAG_C_COL_TYPE_DATE
	             || colType1 == JAG_C_COL_TYPE_DINT
	             || colType1 == JAG_C_COL_TYPE_DBIGINT
	             || colType1 == JAG_C_COL_TYPE_DSMALLINT
	             || colType1 == JAG_C_COL_TYPE_DTINYINT
	             || colType1 == JAG_C_COL_TYPE_DMEDINT
			  ) {
		return JagRange::doRangeIntersect( _jpa, mark1, colType1, srid1, sp1, mark2, colType2, srid2, sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_RANGE
	             || colType2 == JAG_C_COL_TYPE_DOUBLE 
	             || colType2 == JAG_C_COL_TYPE_FLOAT
	             || colType2 == JAG_C_COL_TYPE_DATETIME
	             || colType2 == JAG_C_COL_TYPE_TIME
	             || colType2 == JAG_C_COL_TYPE_DATE
	             || colType2 == JAG_C_COL_TYPE_DINT
	             || colType2 == JAG_C_COL_TYPE_DBIGINT
	             || colType2 == JAG_C_COL_TYPE_DSMALLINT
	             || colType2 == JAG_C_COL_TYPE_DTINYINT
	             || colType2 == JAG_C_COL_TYPE_DMEDINT
			  ) {
		return JagRange::doRangeIntersect( _jpa, mark2, colType2, srid2, sp2, mark1, colType1, srid1, sp1 );
	} else {
		throw 2345;
	}

	// prt(("s2411 colType1=[%s] not handled, false\n", colType1.c_str() ));
	return false;
}


bool BinaryOpNode::doAllNearby( const Jstr& mark1, const Jstr &colType1, int srid1, const JagStrSplit &sp1,
     								   const Jstr& mark2, const Jstr &colType2, int srid2, const JagStrSplit &sp2,
									   const Jstr &carg )
{
	//prt(("s2410 doAllNearby colType1=[%s] \n", colType1.c_str() ));
	//prt(("s2410 colType2=[%s] \n", colType2.c_str() ));
	return JagGeo::doAllNearby( mark1, colType1, srid1, sp1, mark2, colType2, srid2, sp2, carg );
}


bool BinaryOpNode::isAggregateOp( short op )
{
    if (op == JAG_FUNC_MIN || op == JAG_FUNC_MAX || op == JAG_FUNC_AVG || op == JAG_FUNC_COUNT ||
        op == JAG_FUNC_SUM || op == JAG_FUNC_STDDEV || op == JAG_FUNC_FIRST || op == JAG_FUNC_LAST ) {
        return true;
    }
    return false;
}


bool BinaryOpNode::isMathOp( short op ) 
{
	if (JAG_NUM_ADD == op || JAG_NUM_SUB == op || JAG_NUM_MULT == op || 
		JAG_NUM_DIV == op || JAG_NUM_REM == op || JAG_NUM_POW == op) {
		return true;
	}
	return false;
}

bool BinaryOpNode::isCompareOp( short op ) 
{
	if (JAG_FUNC_EQUAL == op || JAG_FUNC_NOTEQUAL == op || JAG_FUNC_LESSTHAN == op || JAG_FUNC_LESSEQUAL == op || 
		JAG_FUNC_GREATERTHAN == op || JAG_FUNC_GREATEREQUAL == op || JAG_FUNC_LIKE == op || JAG_FUNC_MATCH == op) {
		return true;
	}
	return false;
}	

bool BinaryOpNode::isStringOp( short op )
{
	if (op == JAG_FUNC_SUBSTR || op == JAG_FUNC_UPPER || op == JAG_FUNC_LOWER || 
		op == JAG_FUNC_LTRIM || op == JAG_FUNC_RTRIM || op == JAG_FUNC_TRIM) {
		return true;
	}
	return false;
}

bool BinaryOpNode::isSpecialOp( short op )
{
	if (op == JAG_FUNC_MIN || op == JAG_FUNC_MAX || op == JAG_FUNC_FIRST || op == JAG_FUNC_LAST || op == JAG_FUNC_LENGTH) {
		return true;
	}
	return false;
}

bool BinaryOpNode::isTimedateOp( short op )
{
	if (op == JAG_FUNC_SECOND || op == JAG_FUNC_MINUTE || op == JAG_FUNC_HOUR || op == JAG_FUNC_DATE || 
		op == JAG_FUNC_MONTH || op == JAG_FUNC_YEAR || 
		op == JAG_FUNC_DATEDIFF || op == JAG_FUNC_DAYOFMONTH || op == JAG_FUNC_DAYOFWEEK || op == JAG_FUNC_DAYOFYEAR) {
		return true;
	}
	
	return false;
}

short BinaryOpNode::getFuncLength( short op ) 
{
	if ( JAG_FUNC_CURDATE == op ) {
		return JAG_FUNC_CURDATE_LEN;
	} else if ( JAG_FUNC_CURTIME == op ) {
		return JAG_FUNC_CURTIME_LEN;
	} else if ( JAG_FUNC_NOW == op ) {
		return JAG_FUNC_NOW_LEN;
	} else if ( JAG_FUNC_TIME == op ) {
		return JAG_FUNC_NOW_LEN;
	} else if ( JAG_FUNC_PI == op ) {
		return JAG_DOUBLE_FIELD_LEN;
	} else {
		return JAG_FUNC_EMPTYARG_LEN;
	}
}

int BinaryOpNode::getTypeMode( short fop )
{
	int tmode = 0; // string
	if (  fop ==  JAG_FUNC_NUMPOINTS 
		  || fop == JAG_FUNC_NUMSEGMENTS
		  || fop == JAG_FUNC_NUMRINGS
		  || fop == JAG_FUNC_NUMPOLYGONS
		  || fop == JAG_FUNC_NUMINNERRINGS
		  || fop == JAG_FUNC_LENGTH
		  || fop == JAG_FUNC_LINELENGTH
		  || fop == JAG_FUNC_DIMENSION
		  || fop == JAG_FUNC_STRDIFF
		  || fop == JAG_FUNC_SRID
		  || fop == JAG_FUNC_ISCLOSED
		  || fop == JAG_FUNC_ISSIMPLE
		  || fop == JAG_FUNC_ISVALID
		  || fop == JAG_FUNC_ISRING
		  || fop == JAG_FUNC_ISPOLYGONCCW
		  || fop == JAG_FUNC_ISPOLYGONCW
		  || fop == JAG_FUNC_ISCONVEX
		  || fop == JAG_FUNC_WITHIN
		  || fop == JAG_FUNC_CONTAIN
		  || fop == JAG_FUNC_NEARBY
		  || fop == JAG_FUNC_SAME
		  || fop == JAG_FUNC_COVEREDBY
		  || fop == JAG_FUNC_DISJOINT
		  || fop == JAG_FUNC_INTERSECT
		  || fop == JAG_FUNC_COVER
	   ) {
	   tmode = 1; // integer
	 } else if ( fop ==  JAG_FUNC_DISTANCE
	 			 || fop == JAG_FUNC_ANGLE
	 			 || fop == JAG_FUNC_AREA
	 			 || fop == JAG_FUNC_PERIMETER
	 			 || fop == JAG_FUNC_VOLUME
	 			 || fop == JAG_FUNC_XMIN
	 			 || fop == JAG_FUNC_YMIN
	 			 || fop == JAG_FUNC_ZMIN
	 			 || fop == JAG_FUNC_XMAX
	 			 || fop == JAG_FUNC_YMAX
	 			 || fop == JAG_FUNC_ZMAX
	 			 || fop == JAG_FUNC_LOCATEPOINT
	           ) {
	   tmode = 2;  // double
	 } else {
	 }

	 return tmode;
}


