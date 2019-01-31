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

#include <JagParseParam.h>
#include <JagClock.h>
#include <JagLineFile.h>
#include <JagParser.h>

JagParseParam::JagParseParam( const JagParser *jps ) 
{ 
	jagParser = jps;
	_rowHash = NULL;
	_colHash = NULL;
	_lineFile = NULL;
	init( NULL, false ); 
}

JagParseParam::~JagParseParam()
{
	//prt(("s3088 JagParseParam::~JagParseParam() \n" ));
	if ( _rowHash ) delete _rowHash;
	if ( _colHash ) delete _colHash;
	if ( _lineFile ) {
		//prt(("s3082 delete _lineFile\n" ));
		delete _lineFile;
		_lineFile = NULL;
	}
}


void JagParseParam::init( const JagParseAttribute *ijpa, bool needClean )
{
	// clean old tree, vector and hashmap
	//prt(("s8408 JagParseParam::init needClean=%d\n", needClean ));
	if ( needClean ) {
		clean();
	}

	// init all other data members
	// set all bool/abaxint/init type data members to 0
	// set optype, fieldSep, lineSep, quoteSep to default
	// set all data string to empty
	// set jpa if not NULL
	parseOK = impComplete = opcode = 0;
	hasExist = hasColumn = hasWhere = hasGroup = hasHaving = hasOrder = hasLimit = hasTimeout = hasPivot = hasExport = 0;
	hasForce = 0;
	limitStart = limit = timeout = keyLength = valueLength = ovalueLength = timediff = exportType = groupType = isMemTable = 0;
	isChainTable = 0;
	getfileActualData = 0;
	optype=' '; fieldSep=','; lineSep='\n'; quoteSep='\'';
	uid = passwd = dbName = batchFileName = "";
	selectTablistClause = selectColumnClause = selectWhereClause = selectGroupClause = selectHavingClause = "";
	selectOrderClause = selectLimitClause = selectTimeoutClause = selectPivotClause = selectExportClause = "";
	loadColumnClause = loadLineClause = loadQuoteClause = "";
	insertDCSyncHost = "";
	if ( ijpa ) jpa = *ijpa;
	dorep = true;

	origCmd = "";
	dbNameCmd = "";
	origpos = NULL;
	tabidxpos = NULL;
	endtabidxpos = NULL;

	grantPerm = "";
	grantObj = "";
	grantUser = "";
	grantWhere = "";
	detail = false;
	hasPoly = false;
	polyDim = 0;
	_rowHash = NULL;
	_colHash = NULL;
	_lineFile = NULL;
	hasCountAll = false;
	like="";
	_selectStar = false;
	cmd = 0;
}

// clean all trees and reset all vectors
void JagParseParam::clean()
{	
	// clean objectVec, orderVec, groupVec, otherVec, createAttrVec, updSetVec, selColVec, joinOnVec, whereVec, havingVec, offsetVec and map
	objectVec.clean(); orderVec.clean(); groupVec.clean(); otherVec.clean(); createAttrVec.clean(); 
	updSetVec.clean(); selColVec.clean(); joinOnVec.clean(); whereVec.clean(); offsetVec.clean(); 
	inscolmap.removeAllKey(); joincolmap.removeAllKey(); treecheckmap.removeAllKey();
	// havingVec.clean(); 
	selAllColVec.clean();
	jpa.clean();
	_selectStar = false;
	cmd = 0;
	value = "";

	if ( _rowHash ) {
		delete _rowHash;
		_rowHash = NULL;
	}

	if ( _colHash ) {
		delete _colHash;
		_colHash = NULL;
	}

	if ( _lineFile ) {
		// prt(("s0393 JagParseParam::clean() delete _lineFile\n" ));
		delete _lineFile;
		_lineFile = NULL;
	}
}


// method to set select where tree
// return value <= 0 error; 1 OK
int JagParseParam::setSelectWhere( )
{
	//prt(("s4034 setSelectWhere selectWhereClause=[%s] \n", selectWhereClause.c_str() ));
	if ( this->selectWhereClause.length() < 1 ) return -2500;

	const char *pwhere = this->selectWhereClause.c_str();	
	OnlyTreeAttribute ota;
	//prt(("s3348\n" ));
	// ota.init( this->jpa );
	ota.init( this->jpa, this );
	this->whereVec.append( ota );

	int last = this->whereVec.length()-1;
	this->whereVec[last].tree->init( this->jpa, this );
	//prt(("s3341\n" ));
	this->setupCheckMap(); // uses objectVec
	//prt(("s2308 tree->parse(%s) ...\n", pwhere ));
	this->whereVec[last].tree->parse( jagParser, pwhere, 1, this->treecheckmap, this->joincolmap, 
								       this->whereVec[last].colList );
	//prt(("s0183 tree->parse done\n" ));
	this->hasWhere = 1;
	return 1;
}

// method to set select where tree
// return value <= 0 error; 1 OK
// extraWhere: "where1|where2|where3|"
int JagParseParam::resetSelectWhere( const Jstr &extraWhere )
{
	JagHashMap<AbaxString, AbaxInt> hash;
	JagStrSplit sp(extraWhere, '|', true );
	Jstr newWhere;
	for ( int i=0; i < sp.length(); ++i ) {
		if ( sp[i].size() < 1 ) continue;
		if ( ! hash.keyExist( sp[i].c_str() ) ) {
			hash.addKeyValue( sp[i].c_str(), 1 );
			if ( newWhere.size() < 1 ) {
				newWhere = sp[i];
			} else {
				newWhere += Jstr(" and ") + sp[i];
			}
		}
	}

	//prt(("s4035 resetSelectWhere selectWhereClause=[%s] extraWhere=[%s]\n", selectWhereClause.c_str(), extraWhere.c_str() ));
	if ( selectWhereClause.size() > 0 && newWhere.size() > 0 ) {
		selectWhereClause = Jstr("(") + selectWhereClause + ") and (" + newWhere + ")";
	} else if ( newWhere.size() > 0 ) {
		selectWhereClause = newWhere;
	}

	/***
	prt(("s4036 result resetSelectWhere extraWhere=[%s] newWhere=[%s] selectWhereClause=[%s]\n", 
		extraWhere.c_str(), newWhere.c_str(), selectWhereClause.c_str()  ));
	***/

	return 1;
}

// method to setup hashmap of treecheckmap
// return 0: already set, ignore; return 1: set treecheckmap
// treecheckmap: key=db.tab.idx  value: index number in objectVec
// treecheckmap: key=db.idx      value: index number in objectVec
// treecheckmap: key=idx      value: index number in objectVec
// treecheckmap: key=db.tab      value: index number in objectVec
// treecheckmap: key=tab      value: index number in objectVec
int JagParseParam::setupCheckMap()
{
	if ( treecheckmap.size() > 0 ) return 0;
	AbaxString pname, fname;
	AbaxPair<AbaxString, abaxint> pair;
	abaxint cnt = -1;
	for ( int i = 0; i < objectVec.length(); ++i ) {
		pair.value = i;
		if ( JAG_INSERTSELECT_OP == opcode ) pair.value = 0;
		// put "<db.tab<db.tab,cnt>>", "<tab<db.tab,cnt>>" or 
		// "<db.tab.idx<db.tab.idx,cnt>>", "<db.idx<db.tab.idx,cnt>>", "<idx<db.tab.idx,cnt>>" into treecheckmap
		if ( objectVec[i].indexName.length() > 0 ) {
			fname = objectVec[i].dbName + "." + objectVec[i].tableName + "." + objectVec[i].indexName;
			pair.key = fname;
			// "<db.tab.idx<db.tab.idx,cnt>>"
			pname = fname;
			treecheckmap.addKeyValue( pname, pair );
			// "<db.idx<db.tab.idx,cnt>>"
			pname = objectVec[i].dbName + "." + objectVec[i].indexName;
			treecheckmap.addKeyValue( pname, pair );
			// "<idx<db.tab.idx,cnt>>"
			pname = objectVec[i].indexName;
			treecheckmap.addKeyValue( pname, pair );
		} else {
			fname = objectVec[i].dbName + "." + objectVec[i].tableName;
			pair.key = fname;
			// "<db.tab<db.tab,cnt>>"
			pname = fname;
			treecheckmap.addKeyValue( pname, pair );
			// "<tab<db.tab,cnt>>"
			pname = objectVec[i].tableName;
			treecheckmap.addKeyValue( pname, pair );
		}
	}

	pair.key = fname;
	pair.value = objectVec.length();
	treecheckmap.addKeyValue( "0", pair );
	return 1;
}

//////////////////////////// OtherAttribute ///////////////////
OtherAttribute::OtherAttribute()
{
	//mpgon = NULL;
	init();
}

OtherAttribute::~OtherAttribute()
{
	//if ( mpgon ) delete mpgon;
}

void OtherAttribute::init( ) 
{
	objName.init(); 
	valueData = ""; hasQuote = 0; type=""; issubcol = false;
	point.init(); linestr.init(); 
	is3D = false;
}

void OtherAttribute::copyData( const OtherAttribute& other )
{
		hasQuote = other.hasQuote;
		objName = other.objName;
		valueData = other.valueData;
		type = other.type;
		point = other.point;
		linestr = other.linestr;
		//pgon = other.pgon;
		issubcol = other.issubcol;
}

OtherAttribute& OtherAttribute::operator=( const OtherAttribute& other )
{
		if ( this == &other ) return *this;
		copyData( other );
		return *this;
}

OtherAttribute::OtherAttribute ( const OtherAttribute& other )
{
		copyData( other );
}

void OtherAttribute::print()
{
	prt(("s6810 OtherAttribute::print():\n" ));
	prt(("  objName=[%s]\n", objName.colName.c_str() ));
	prt(("  valueData=[%s]\n", valueData.c_str() ));
	prt(("  type=[%s]\n", type.c_str() ));
	prt(("  issubcol=[%d]\n", issubcol ));
	prt(("  linestr.size=[%d]\n", linestr.size() ));
}


//////////////////////////// OnlyTreeAttribute //////////////
void OnlyTreeAttribute::init( const JagParseAttribute &jpa, JagParseParam *pram ) 
{
 	destroy();
	tree = new BinaryExpressionBuilder();
	tree->init( jpa, pram ); 
}

void OnlyTreeAttribute::destroy() 
{
	if ( tree ) {
		ExprElementNode *root = tree->getRoot();
		if ( root ) { 
			root->clear(); 
			delete root; 
		}

		tree->clean();
		delete tree;
		tree = NULL;
	} 
}

// rebuild select sql from internal saved statements
Jstr JagParseParam::formSelectSQL()
{
	Jstr q;
	formatInsertSelectCmdHeader( this, q );
	q += "select " + selectColumnClause + " from " + selectTablistClause + " ";

	if ( hasWhere ) {
		q += Jstr(" where ") + selectWhereClause + " ";
	}

	if ( hasGroup ) {
		q += Jstr(" group by ") + selectGroupClause + " ";
	}

	if ( hasOrder ) {
		q += Jstr(" order by ") +  selectOrderClause + " ";
	}

	if ( hasLimit ) {
		q += Jstr(" limit ") +  selectLimitClause + " ";
	}

	if ( hasTimeout ) {
		q += Jstr(" timeout ") +  selectTimeoutClause + " ";
	}

	// prt(("s3028 selectExportClause=[%s]\n", selectExportClause.c_str() ));
	if ( hasExport ) {
		q +=  selectExportClause;
	}

	return q;
}

void JagParseParam::print()
{
	prt(("\n\n\n\n\ns3481 --------------------print parse param----------------------------\n"));
	prt(("jpa.timediff=[%d] jpa.servtimediff=[%d] jpa.dfdbname=[%s]\n", 
			this->jpa.timediff, this->jpa.servtimediff, this->jpa.dfdbname.c_str()));
	prt(("parseOK=[%d] impComplete=[%d] opcode=[%d] optype=[%c]\n", 
			this->parseOK, this->impComplete, this->opcode, this->optype));
	prt(("hasExist=[%d] hasColumn=[%d] hasWhere=[%d] hasForce=[%d]\n", 
			this->hasExist, this->hasColumn, this->hasWhere, this->hasForce ));
	prt(("hasGroup=[%d] hasHaving=[%d] hasOrder=[%d]\n", this->hasGroup, this->hasHaving, this->hasOrder));
	prt(("hasLimit=[%d] hasTimeout=[%d] hasPivot=[%d] hasExport=[%d]\n", 
		  this->hasLimit, this->hasTimeout, this->hasPivot, this->hasExport));
	prt(("limitStart=[%d] limit=[%d] timeout=[%d]\n", this->limitStart, this->limit, this->timeout));
	prt(("keyLength=[%d] valueLength=[%d] ovalueLength=[%d]\n", 
			this->keyLength, this->valueLength, this->ovalueLength));
	prt(("timediff=[%d] exportType=[%d] groupType=[%d] isMemTable=[%d]\n", 
		 this->timediff, this->exportType, this->groupType, this->isMemTable));
	prt(("fieldSep=[%c] ", this->fieldSep ));
	prt(("lineSep=[%c] ", this->lineSep ));
	prt(("quoteSep=[%c]\n", this->quoteSep ));
	prt(("uid=[%s] passwd=[%s] ", this->uid.c_str(), this->passwd.c_str()));
	prt(("dbName=[%s] batchFileName=[%s]\n", this->dbName.c_str(), this->batchFileName.c_str()));
	prt(("origCmd=[%s]\n", this->origCmd.c_str()));
	prt(("dbNameCmd=[%s]\n", this->dbNameCmd.c_str()));
	prt(("selectTablistClause=[%s]\n", this->selectTablistClause.c_str()));
	prt(("selectColumnClause=[%s]\n", this->selectColumnClause.c_str()));
	prt(("selectWhereClause=[%s]\n", this->selectWhereClause.c_str()));
	prt(("selectGroupClause=[%s]\n", this->selectGroupClause.c_str()));
	prt(("selectHavingClause=[%s]\n", this->selectHavingClause.c_str()));
	prt(("selectOrderClause=[%s]\n", this->selectOrderClause.c_str()));
	prt(("selectLimitClause=[%s]\n", this->selectLimitClause.c_str()));
	prt(("selectTimeoutClause=[%s]\n", this->selectTimeoutClause.c_str()));
	prt(("selectPivotClause=[%s]\n", this->selectPivotClause.c_str()));
	prt(("selectExportClause=[%s]\n", this->selectExportClause.c_str()));
	prt(("loadColumnClause=[%s]\n", this->loadColumnClause.c_str()));
	prt(("loadLineClause=[%s]\n", this->loadLineClause.c_str()));
	prt(("loadQuoteClause=[%s]\n", this->loadQuoteClause.c_str()));
	
	if ( this->objectVec.size() > 0 ) {
		prt(("-----------------object vector------------------\n"));
		for ( int i = 0; i < this->objectVec.size(); ++i ) {
			prt(("dbName=[%s]   ", this->objectVec[i].dbName.c_str()));
			prt(("tableName=[%s]   ", this->objectVec[i].tableName.c_str()));
			prt(("indexName=[%s]   ", this->objectVec[i].indexName.c_str()));
			prt(("colName=[%s]\n", this->objectVec[i].colName.c_str()));
		}	
	}

	if ( this->orderVec.size() > 0 ) {
		prt(("-----------------order vector------------------\n"));
		for ( int i = 0; i < this->orderVec.size(); ++i ) {
			prt(("name=[%s]   ", this->orderVec[i].name.c_str()));
			prt(("isasc=[%d]\n", this->orderVec[i].isAsc));
		}	
	}

	if ( this->groupVec.size() > 0 ) {
		prt(("-----------------group vector------------------\n"));
		for ( int i = 0; i < this->groupVec.size(); ++i ) {
			prt(("name=[%s]   ", this->groupVec[i].name.c_str()));
			prt(("isasc=[%d]\n", this->groupVec[i].isAsc));
		}
	}

	if ( this->otherVec.size() > 0 ) {
		prt(("-----------------other vector------------------\n"));
		for ( int i = 0; i < this->otherVec.size(); ++i ) {
			prt(("dbName=[%s]   ", this->otherVec[i].objName.dbName.c_str()));
			prt(("tableName=[%s]   ", this->otherVec[i].objName.tableName.c_str()));
			prt(("indexName=[%s]   ", this->otherVec[i].objName.indexName.c_str()));
			prt(("colName=[%s]   ", this->otherVec[i].objName.colName.c_str()));
			prt(("valueData=[%s]\n", this->otherVec[i].valueData.c_str()));
		}
	}

	if ( this->createAttrVec.size() > 0 ) {
		prt(("-----------------create vector------------------\n"));
		for ( int i = 0; i < this->createAttrVec.size(); ++i ) {
			prt(("dbName=[%s]   ", this->createAttrVec[i].objName.dbName.c_str()));
			prt(("tableName=[%s]   ", this->createAttrVec[i].objName.tableName.c_str()));
			prt(("indexName=[%s]   ", this->createAttrVec[i].objName.indexName.c_str()));
			prt(("colName=[%s]   ", this->createAttrVec[i].objName.colName.c_str()));
			prt(("spare=[%c", this->createAttrVec[i].spare[0]));
			prt(("%c", this->createAttrVec[i].spare[1]));
			prt(("%c", this->createAttrVec[i].spare[2]));
			prt(("%c", this->createAttrVec[i].spare[3]));
			prt(("%c", this->createAttrVec[i].spare[4]));
			prt(("%c", this->createAttrVec[i].spare[5]));
			prt(("%c", this->createAttrVec[i].spare[6]));
			prt(("%c", this->createAttrVec[i].spare[7]));
			prt(("%c]   ", this->createAttrVec[i].spare[8]));
			prt(("type=[%s]   ", this->createAttrVec[i].type.c_str()));
			prt(("offset=[%d]   ", this->createAttrVec[i].offset));
			prt(("length=[%d]   ", this->createAttrVec[i].length));
			prt(("sig=[%d]\n", this->createAttrVec[i].sig));
		}
	}

	if ( this->updSetVec.size() > 0 ) {
		prt(("-----------------update set vector------------------\n"));
		for ( int i = 0; i < this->updSetVec.size(); ++i ) {
			ExprElementNode *root = this->updSetVec[i].tree->getRoot();
			if ( root ) root->print(0); 
			prt(("\ncolName=[%s]\n", this->updSetVec[i].colName.c_str()));
		}
	}	

	if ( this->selColVec.size() > 0 ) {
		prt(("-----------------select column vector------------------\n"));
		for ( int i = 0; i < this->selColVec.size(); ++i ) {
			ExprElementNode *root = this->selColVec[i].tree->getRoot();
			if ( root ) root->print(0);		
			prt(("\norigFuncStr=[%s]   ", this->selColVec[i].origFuncStr.c_str()));
			prt(("asName=[%s]   ", this->selColVec[i].asName.c_str()));
			prt(("offset=[%d]   ", this->selColVec[i].offset));
			prt(("length=[%d]   ", this->selColVec[i].length));
			prt(("sig=[%d]   ", this->selColVec[i].sig));
			prt(("type=[%s]   ", this->selColVec[i].type.c_str()));
			prt(("isAggregate=[%d]   ", this->selColVec[i].isAggregate));
			prt(("strResult=[%s] %0x %0x\n", this->selColVec[i].strResult.c_str(), this->selColVec[i].tree, this));
		}	
	}

	if ( this->selAllColVec.size() > 0 ) {
		prt(("-----------------select all column vector------------------\n"));
		for ( int i = 0; i < this->selAllColVec.size(); ++i ) {
			prt(("i=%d name=[%s]\n", i, this->selAllColVec[i].c_str() ));
		}
	}

	if ( this->joinOnVec.size() > 0 ) {
		prt(("-----------------select join on vector------------------\n"));
		for ( int i = 0; i < this->joinOnVec.size(); ++i ) {
			ExprElementNode *root = this->joinOnVec[i].tree->getRoot();
			if ( root ) root->print(0);
			prt(("\n"));
		}
	}

	if ( this->whereVec.size() > 0 ) {
		prt(("-----------------select where vector------------------\n"));
		for ( int i = 0; i < this->whereVec.size(); ++i ) {
			ExprElementNode *root = this->whereVec[i].tree->getRoot();
			if ( root ) root->print(0);
			prt(("\n"));
		}
	}	

	/***
	if ( this->havingVec.size() > 0 ) {
		prt(("-----------------select having vector------------------\n"));
		for ( int i = 0; i < this->havingVec.size(); ++i ) {
			ExprElementNode *root = this->havingVec[i].tree->getRoot();
			if ( root ) root->print(0);
			prt(("\n"));
		}
	}
	***/

	if ( this->offsetVec.size() > 0 ) {
		prt(("-----------------offset vector------------------\n"));
		for ( int i = 0; i < this->offsetVec.size(); ++i ) {
			prt(("offset=[%d]\n", this->offsetVec[i]));
		}
	}

	if ( this->inscolmap.size() > 0 ) {
		prt(("-----------------insert column map------------------\n"));
		// this->inscolmap.printKeyStringValueInteger();
		prt(("\n"));
	}

	if ( this->joincolmap.size() > 0 ) {
		prt(("-----------------join column map------------------\n"));
		// this->joincolmap.printKeyStringValueInteger();
		prt(("\n"));
	}

	if ( this->treecheckmap.size() > 0 ) {
		prt(("-----------------tree check map------------------\n"));
		// this->treecheckmap.printKeyStringOnly();
		prt(("\n"));
	}

}

// add double :
// add x y columns
int JagParseParam::addPointColumns( const CreateAttribute &pointcattr )
{
	int iskey = false;
	if ( *(pointcattr.spare) == JAG_C_COL_KEY ) {
		iskey = true;
	}

	int offset = pointcattr.offset;
	CreateAttribute cattr;
	cattr.objName.colName = pointcattr.objName.colName + ":x";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":y";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	return 2;
}


// add x y z columns
int JagParseParam::addPoint3DColumns( const CreateAttribute &pointcattr )
{
	int iskey = false;
	if ( *(pointcattr.spare) == JAG_C_COL_KEY ) {
		iskey = true;
	}

	int offset = pointcattr.offset;
	CreateAttribute cattr;
	cattr.objName.colName = pointcattr.objName.colName + ":x";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":y";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":z";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	return 3;
}


// add x y r columns
int JagParseParam::addCircleColumns( const CreateAttribute &pointcattr )
{
	int iskey = false;
	if ( *(pointcattr.spare) == JAG_C_COL_KEY ) {
		iskey = true;
	}

	int offset = pointcattr.offset;
	CreateAttribute cattr;
	cattr.objName.colName = pointcattr.objName.colName + ":x";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":y";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":a";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	return 3;
}

// add x y a nx columns
int JagParseParam::addSquareColumns( const CreateAttribute &pointcattr )
{
	int iskey = false;
	if ( *(pointcattr.spare) == JAG_C_COL_KEY ) {
		iskey = true;
	}

	int offset = pointcattr.offset;
	CreateAttribute cattr;
	cattr.objName.colName = pointcattr.objName.colName + ":x";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":y";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":a";
	fillDoubleSubData( cattr, offset, iskey, 0 );
	cattr.objName.colName = pointcattr.objName.colName + ":nx";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	return 3;
}

// add x y width height columns
int JagParseParam::addColumns( const CreateAttribute &pointcattr,
					bool hasX, bool hasY, bool hasZ, 
					bool hasWidth, bool hasDepth, bool hasHeight,
					bool hasNX, bool hasNY )
{
	int iskey = false;
	if ( *(pointcattr.spare) == JAG_C_COL_KEY ) {
		iskey = true;
	}

	int offset = pointcattr.offset;
	CreateAttribute cattr;
	if ( hasX ) {
    	cattr.objName.colName = pointcattr.objName.colName + ":x";
		fillDoubleSubData( cattr, offset, iskey, 0 );
	}
    
	if ( hasY ) {
    	cattr.objName.colName = pointcattr.objName.colName + ":y";
		fillDoubleSubData( cattr, offset, iskey, 0 );
	}
    
	if ( hasZ ) {
    	cattr.objName.colName = pointcattr.objName.colName + ":z";
		fillDoubleSubData( cattr, offset, iskey, 0 );
	}

	if ( hasWidth ) {
    	cattr.objName.colName = pointcattr.objName.colName + ":a";
		fillDoubleSubData( cattr, offset, iskey, 0 );
	}
    
	if ( hasDepth ) {
    	cattr.objName.colName = pointcattr.objName.colName + ":b";
		fillDoubleSubData( cattr, offset, iskey, 0 );
	}

	if ( hasHeight ) {
    	cattr.objName.colName = pointcattr.objName.colName + ":c";
		fillDoubleSubData( cattr, offset, iskey, 0 );
	}
    
	if ( hasNX ) {
    	cattr.objName.colName = pointcattr.objName.colName + ":nx";
		fillDoubleSubData( cattr, offset, iskey, 0 );
	}
    
	if ( hasNY ) {
    	cattr.objName.colName = pointcattr.objName.colName + ":ny";
		fillDoubleSubData( cattr, offset, iskey, 0 );
	}

	return 4;
}


// add x y z width depth height columns
int JagParseParam::addBoxColumns( const CreateAttribute &pointcattr )
{
	int iskey = false;
	if ( *(pointcattr.spare) == JAG_C_COL_KEY ) {
		iskey = true;
	}

	CreateAttribute cattr;
	int offset = pointcattr.offset;

	cattr.objName.colName = pointcattr.objName.colName + ":x";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":y";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":z";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":a";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":b";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":c";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":nx";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":ny";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	return 6;
}

// add x y z r height columns
int JagParseParam::addCylinderColumns( const CreateAttribute &pointcattr )
{
	int iskey = false;
	if ( *(pointcattr.spare) == JAG_C_COL_KEY ) {
		iskey = true;
	}

	CreateAttribute cattr;
	int offset = pointcattr.offset;

	cattr.objName.colName = pointcattr.objName.colName + ":x";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	// y col
	cattr.objName.colName = pointcattr.objName.colName + ":y";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	// z col
	cattr.objName.colName = pointcattr.objName.colName + ":z";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	// r col
	cattr.objName.colName = pointcattr.objName.colName + ":a";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	// height col
	cattr.objName.colName = pointcattr.objName.colName + ":c";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":nx";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":ny";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	return 5;
}

// optype is 'R' 'W' 'C' 'D'
// return 1: simple cmds, with optype=='D' : show/desc etc
// return 2: schema/userid changed cmd, with optype=='C' : create/drop/truncate/alter/import etc.
// return 3, 4: write related cmd, with optype=='W' : 3 for insert/cinsert and 4 for update/delete
// return 5, 6: read related cmd, with optype=='R' : 5 for select/count and 6 for join/starjoin/indexjoin
// return 0: others invalid
short JagParseParam::checkCmdMode()
{
	if ( optype == 'D' ) return 1;
	else if ( optype == 'C' ) return 2;
	else if ( optype == 'W' ) {
		if ( JAG_INSERTSELECT_OP == opcode || JAG_INSERT_OP == opcode || JAG_CINSERT_OP == opcode ) return 3;
		else if ( JAG_UPDATE_OP == opcode || JAG_DELETE_OP == opcode ) return 4;
		return 0;
	} else if ( optype == 'R' ) {
		if ( JAG_SELECT_OP == opcode || JAG_GETFILE_OP == opcode || JAG_COUNT_OP == opcode ) return 5;
		else if ( isJoin( opcode ) ) return 6;
		return 0;
	}
	return 0;
}

void JagParseParam::fillDoubleSubData( CreateAttribute &cattr, int &offset, int iskey, int isMute, int isSub )
{
	cattr.spare[JAG_SCHEMA_SPARE_LEN] = '\0';
	memset( cattr.spare, JAG_S_COL_SPARE_DEFAULT, JAG_SCHEMA_SPARE_LEN );
	cattr.type = JAG_C_COL_TYPE_DOUBLE;
	cattr.offset = offset;
	cattr.length = JAG_GEOM_TOTLEN;		
	cattr.sig = JAG_GEOM_PRECISION;
	if ( iskey ) {
		*(cattr.spare) = JAG_C_COL_KEY;
		//this->keyLength += 1+cattr.length;
		this->keyLength += cattr.length;
	} else {
		*(cattr.spare) = JAG_C_COL_VALUE;
		//this->valueLength += 1+cattr.length;
		this->valueLength += cattr.length;
	}
	*(cattr.spare+2) = JAG_ASC;
	if ( isSub ) {
		*(cattr.spare+6) = JAG_SUB_COL;
	}

	if ( isMute ) {
		*(cattr.spare+5) = JAG_KEY_MUTE;
	}

	this->createAttrVec.append( cattr );
	cattr.init();
	offset += JAG_GEOM_TOTLEN;
}

// modiies cattr,  this->createAttrVec this->keyLength this->valueLength
void JagParseParam::fillBigintSubData( CreateAttribute &cattr, int &offset, int iskey, int isMute, int isSub )
{
	cattr.spare[JAG_SCHEMA_SPARE_LEN] = '\0';
	memset( cattr.spare, JAG_S_COL_SPARE_DEFAULT, JAG_SCHEMA_SPARE_LEN );
	cattr.type = JAG_C_COL_TYPE_DBIGINT;
	cattr.offset = offset;
	cattr.length = JAG_DBIGINT_FIELD_LEN;		
	cattr.sig = 0;
	if ( iskey ) {
		*(cattr.spare) = JAG_C_COL_KEY;
		//this->keyLength += 1+cattr.length;
		this->keyLength += cattr.length;
	} else {
		*(cattr.spare) = JAG_C_COL_VALUE;
		//this->valueLength += 1+cattr.length;
		this->valueLength += cattr.length;
	}

	if ( iskey && isMute ) {
		*(cattr.spare+5) = JAG_KEY_MUTE;
	}

	*(cattr.spare+2) = JAG_ASC;
	if ( isSub ) {
		*(cattr.spare+6) = JAG_SUB_COL;
	}
	this->createAttrVec.append( cattr );
	cattr.init();
	offset += JAG_DBIGINT_FIELD_LEN;
}

// modifies cattr,  this->createAttrVec this->keyLength this->valueLength
void JagParseParam::fillIntSubData( CreateAttribute &cattr, int &offset, int iskey, int isMute, int isSub )
{
	cattr.spare[JAG_SCHEMA_SPARE_LEN] = '\0';
	memset( cattr.spare, JAG_S_COL_SPARE_DEFAULT, JAG_SCHEMA_SPARE_LEN );
	cattr.type = JAG_C_COL_TYPE_DINT;
	cattr.offset = offset;
	cattr.length = JAG_DINT_FIELD_LEN;		
	cattr.sig = 0;
	if ( iskey ) {
		*(cattr.spare) = JAG_C_COL_KEY;
		//this->keyLength += 1+cattr.length;
		this->keyLength += cattr.length;
	} else {
		*(cattr.spare) = JAG_C_COL_VALUE;
		//this->valueLength += 1+cattr.length;
		this->valueLength += cattr.length;
	}

	if ( iskey && isMute ) {
		*(cattr.spare+5) = JAG_KEY_MUTE;
	}

	*(cattr.spare+2) = JAG_ASC;
	if ( isSub ) {
		*(cattr.spare+6) = JAG_SUB_COL;
	}
	this->createAttrVec.append( cattr );
	cattr.init();
	offset += JAG_DINT_FIELD_LEN;
}

// modifies cattr,  this->createAttrVec this->keyLength this->valueLength
void JagParseParam::fillSmallIntSubData( CreateAttribute &cattr, int &offset, int iskey, int isMute, int isSub )
{
	cattr.spare[JAG_SCHEMA_SPARE_LEN] = '\0';
	memset( cattr.spare, JAG_S_COL_SPARE_DEFAULT, JAG_SCHEMA_SPARE_LEN );
	cattr.type = JAG_C_COL_TYPE_DSMALLINT;
	cattr.offset = offset;
	cattr.length = JAG_DSMALLINT_FIELD_LEN;		
	cattr.sig = 0;
	if ( iskey ) {
		*(cattr.spare) = JAG_C_COL_KEY;
		//this->keyLength += 1+cattr.length;
		this->keyLength += cattr.length;
	} else {
		*(cattr.spare) = JAG_C_COL_VALUE;
		//this->valueLength += 1+cattr.length;
		this->valueLength += cattr.length;
	}

	if ( iskey && isMute ) {
		*(cattr.spare+5) = JAG_KEY_MUTE;
	}

	*(cattr.spare+2) = JAG_ASC;
	if ( isSub ) {
		*(cattr.spare+6) = JAG_SUB_COL;
	}
	this->createAttrVec.append( cattr );
	cattr.init();
	offset += JAG_DSMALLINT_FIELD_LEN;
}


void JagParseParam::fillStringSubData( CreateAttribute &cattr, int &offset, int isKey, int len, int isMute, int isSub )
{
	cattr.spare[JAG_SCHEMA_SPARE_LEN] = '\0';
	memset( cattr.spare, JAG_S_COL_SPARE_DEFAULT, JAG_SCHEMA_SPARE_LEN );
	cattr.type = JAG_C_COL_TYPE_STR;
	cattr.offset = offset;
	cattr.length = len;		
	cattr.sig = 0;
	if ( isKey ) {
		*(cattr.spare) = JAG_C_COL_KEY;
		//this->keyLength += 1+cattr.length;
		this->keyLength += cattr.length;
	} else {
		*(cattr.spare) = JAG_C_COL_VALUE;
		//this->valueLength += 1+cattr.length;
		this->valueLength += cattr.length;
	}

	*(cattr.spare+2) = JAG_RAND;
	if ( isSub ) {
		*(cattr.spare+6) = JAG_SUB_COL;
	}

	if ( isMute ) {
		*(cattr.spare+5) = JAG_KEY_MUTE;
	}

	this->createAttrVec.append( cattr );
	cattr.init();
	offset += len;
}

void JagParseParam::fillRangeSubData( int colLen, CreateAttribute &cattr, int &offset, int iskey, int isSub )
{
	cattr.spare[JAG_SCHEMA_SPARE_LEN] = '\0';
	memset( cattr.spare, JAG_S_COL_SPARE_DEFAULT, JAG_SCHEMA_SPARE_LEN );

	Jstr ctype = JagParser::getFieldType( cattr.srid ); 
	cattr.type = ctype;
	//prt(("s3387 fillRangeSubData srid=%d ctype=[%s]\n", cattr.srid, ctype.c_str() ));

	cattr.offset = offset;
	cattr.length = colLen;		

	if ( JAG_RANGE_DOUBLE == cattr.srid ) {
		cattr.sig = JAG_DOUBLE_SIG_LEN;
	} else if ( JAG_RANGE_FLOAT == cattr.srid ) {
		cattr.sig = JAG_FLOAT_SIG_LEN;
	} else {
		cattr.sig = 0;
	}

	if ( iskey ) {
		*(cattr.spare) = JAG_C_COL_KEY;
		this->keyLength += cattr.length;
	} else {
		*(cattr.spare) = JAG_C_COL_VALUE;
		this->valueLength += cattr.length;
	}
	*(cattr.spare+2) = JAG_ASC;
	if ( isSub ) {
		*(cattr.spare+6) = JAG_SUB_COL;
	}
	this->createAttrVec.append( cattr );
	cattr.init();
	offset += colLen;
}

// add x1 y1 z1 x2 y2 z2  columns
int JagParseParam::addLineColumns( const CreateAttribute &pointcattr, bool is3D )
{
	int iskey = false;
	if ( *(pointcattr.spare) == JAG_C_COL_KEY ) {
		iskey = true;
	}

	CreateAttribute cattr;
	int offset = pointcattr.offset;

	cattr.objName.colName = pointcattr.objName.colName + ":x1";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":y1";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	if ( is3D ) {
		cattr.objName.colName = pointcattr.objName.colName + ":z1";
		fillDoubleSubData( cattr, offset, iskey, 0 );
	}

	cattr.objName.colName = pointcattr.objName.colName + ":x2";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":y2";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	if ( is3D ) {
		cattr.objName.colName = pointcattr.objName.colName + ":z2";
		fillDoubleSubData( cattr, offset, iskey, 0 );
	}

	return 4;
}

// add  x y columns
int JagParseParam::addLineStringColumns( const CreateAttribute &pointcattr, bool is3D )
{
	int iskey = false;
	if ( *(pointcattr.spare) == JAG_C_COL_KEY ) {
		iskey = true;
	}

	CreateAttribute cattr;
	int offset = pointcattr.offset;

	cattr.objName.colName = pointcattr.objName.colName + ":x";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":y";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	if ( is3D ) {
		cattr.objName.colName = pointcattr.objName.colName + ":z";
		fillDoubleSubData( cattr, offset, iskey, 0 );
	}

	return 4;
}

// add  x y columns
int JagParseParam::addPolygonColumns( const CreateAttribute &pointcattr, bool is3D )
{
	int iskey = false;
	if ( *(pointcattr.spare) == JAG_C_COL_KEY ) {
		iskey = true;
	}

	CreateAttribute cattr;
	int offset = pointcattr.offset;

	cattr.objName.colName = pointcattr.objName.colName + ":x";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":y";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	if ( is3D ) {
		cattr.objName.colName = pointcattr.objName.colName + ":z";
		fillDoubleSubData( cattr, offset, iskey, 0 );
	}

	return 4;
}


// add x1 y1 z1 x2 y2 z2  columns
int JagParseParam::addTriangleColumns( const CreateAttribute &pointcattr, bool is3D )
{
	int iskey = false;
	if ( *(pointcattr.spare) == JAG_C_COL_KEY ) {
		iskey = true;
	}

	CreateAttribute cattr;
	int offset = pointcattr.offset;

	cattr.objName.colName = pointcattr.objName.colName + ":x1";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":y1";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	if ( is3D ) {
		cattr.objName.colName = pointcattr.objName.colName + ":z1";
		fillDoubleSubData( cattr, offset, iskey, 0 );
	}

	cattr.objName.colName = pointcattr.objName.colName + ":x2";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":y2";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	if ( is3D ) {
		cattr.objName.colName = pointcattr.objName.colName + ":z2";
		fillDoubleSubData( cattr, offset, iskey, 0 );
	}

	cattr.objName.colName = pointcattr.objName.colName + ":x3";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	cattr.objName.colName = pointcattr.objName.colName + ":y3";
	fillDoubleSubData( cattr, offset, iskey, 0 );

	if ( is3D ) {
		cattr.objName.colName = pointcattr.objName.colName + ":z3";
		fillDoubleSubData( cattr, offset, iskey, 0 );
	}

	return 4;
}


// add  begin end columns
int JagParseParam::addRangeColumns( int colLen, const CreateAttribute &pointcattr )
{
	int iskey = false;
	if ( *(pointcattr.spare) == JAG_C_COL_KEY ) {
		iskey = true;
	}

	CreateAttribute cattr;
	int offset = pointcattr.offset;
	cattr.srid = pointcattr.srid;
	//prt(("s3832 addRangeColumns srid=%d\n", cattr.srid ));

	cattr.objName.colName = pointcattr.objName.colName + ":begin";
	fillRangeSubData( colLen, cattr, offset, iskey );

	cattr.srid = pointcattr.srid;
	cattr.objName.colName = pointcattr.objName.colName + ":end";
	fillRangeSubData( colLen, cattr, offset, iskey );

	return 2;
}

void JagParseParam::clearRowHash()
{
    if ( ! hasWhere ) return;
    ExprElementNode *root = whereVec[0].tree->getRoot();
    if ( root && root->_builder && root->_builder->_pparam->_rowHash ) {
        delete root->_builder->_pparam->_rowHash;
        root->_builder->_pparam->_rowHash = NULL;
	}
}

void JagParseParam::initColHash()
{
	if ( _colHash ) return;
	_colHash = new JagHashStrStr();
}

bool JagParseParam::isSelectConst() const
{
	//prt(("s1773 _allColumns.size()=%d _selectStar=%d objectVec.size()=%d\n", _allColumns.size(), _selectStar, objectVec.size() ));
	if ( _allColumns.size() < 1 && ! _selectStar && objectVec.size() < 1 ) {
		//prt(("s2838 true\n" ));
		return true;
	} else {
		return false;
	}
}
