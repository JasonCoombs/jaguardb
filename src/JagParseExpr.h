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
#ifndef _ParseExpr_h_
#define _ParseExpr_h_

#include <stdio.h>
#include <stack>
#include <string>
#include <regex>
#include <abax.h>

#include <abax.h>
#include <JagDef.h>
#include <JagHashMap.h>
#include <JagColumn.h>
#include <JagSchemaRecord.h>
#include <JagVector.h>
#include <JagStack.h>
#include <JagUtil.h>
#include <JagTableUtil.h>
#include <JagHashStrInt.h>
#include <JagMergeReader.h>
#include <JagMergeBackReader.h>
#include <JagHashStrStr.h>

/**
				---------------------------
				| ExpressionElementNode    |
				---------------------------
					|			 	     |
					|	  			     |
					|				     |
					|	             -------------------------
					|	             |  BinaryOpNode  |
					|	             -------------------------
		-------------------------
	   |   StringElementNode     |
		-------------------------
**/

class JagParser;
class JagHashStrStr;
class BinaryExpressionBuilder;

class JagBox 
{
  public:
  	int col;
	int offset;
	int len;
};

class ExpressionElementNode
{
  public:
	ExpressionElementNode();
	virtual ~ExpressionElementNode();
	virtual int getBinaryOp() = 0;
	virtual int getName( const char *&p ) = 0;
	virtual bool getValue( const char *&p ) = 0;
	virtual void clear() = 0;
	virtual void print( int mode ) = 0;
	
	// for select/where/on tree use
	virtual int setWhereRange( const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
								const int keylen[], const int numKeys[], int numTabs, bool &hasValue, 
								JagMinMax *minmaxbuf, AbaxFixString &str, int &typeMode, int &tabnum ) = 0;
	virtual int setFuncAttribute( const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
									int &constMode, int &typeMode, bool &isAggregate, AbaxDataString &type, 
									int &collen, int &siglen ) = 0;
	virtual int getFuncAggregate( JagVector<AbaxDataString> &selectParts, JagVector<int> &selectPartsOpcode,
									JagVector<int> &selColSetAggParts, JagHashMap<AbaxInt, AbaxInt> &selColToselParts, 
									int &nodenum, int treenum ) = 0;
	virtual int getAggregateParts( AbaxDataString &parts, int &nodenum ) = 0;
	virtual int setAggregateValue( int nodenum, const char *buf, int length ) = 0;
	virtual int checkFuncValid( JagMergeReaderBase *ntr,  const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[],
								const char *buffers[], AbaxFixString &str, int &typeMode, AbaxDataString &type, 
								int &length, bool &first, bool useZero, bool setGlobal ) = 0;
	virtual int checkFuncValidConstantOnly( AbaxFixString &str, int &typeMode, AbaxDataString &type, int &length ) = 0;

	JagParseAttribute   		_jpa;
	BinaryExpressionBuilder* 	_builder;
	bool            			_isElement;
	AbaxDataString				_name;
	unsigned int				_srid;
	AbaxDataString				_type;
};

class StringElementNode: public ExpressionElementNode
{
  public:
	StringElementNode();
	StringElementNode( BinaryExpressionBuilder* builder,  const AbaxDataString &name, const AbaxFixString &value, 
					   const AbaxDataString &columns, const JagParseAttribute &jpa, int tabnum, int typeMode=0 ) ;
	virtual ~StringElementNode();
	virtual int getBinaryOp() { return 0; }
	virtual int getName( const char *&p ) { if ( _name.length() > 0 ) { p = _name.c_str(); return _tabnum; } return -1; }
	virtual bool getValue( const char *&p ) { if ( _value.length() > 0 ) { p = _value.c_str(); return 1; } return 0; }
	virtual void clear() {}
	virtual void print( int mode );
	
	// for select/where/on tree use
	virtual int setWhereRange( const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
								const int keylen[], const int numKeys[], int numTabs, bool &hasValue, 
								JagMinMax *minmaxbuf, AbaxFixString &str, int &typeMode, int &tabnum );
	virtual int setFuncAttribute( const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
								  int &constMode, int &typeMode, bool &isAggregate, AbaxDataString &type, int &collen, int &siglen );
	virtual int getFuncAggregate( JagVector<AbaxDataString> &selectParts, JagVector<int> &selectPartsOpcode, 
								  JagVector<int> &selColSetAggParts, JagHashMap<AbaxInt, AbaxInt> &selColToselParts, 
								  int &nodenum, int treenum );
	virtual int getAggregateParts( AbaxDataString &parts, int &nodenum );
	virtual int setAggregateValue( int nodenum, const char *buf, int length );
	virtual int checkFuncValid(JagMergeReaderBase *ntr, const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[],
								const char *buffers[], AbaxFixString &str, int &typeMode, AbaxDataString &type, 
								int &length, bool &first, bool useZero, bool setGlobal );
	virtual int checkFuncValidConstantOnly( AbaxFixString &str, int &typeMode, AbaxDataString &type, int &length );
	void makeDataString( const JagSchemaAttribute *attrs[], const char *buffers[], 
						 const AbaxDataString &colobjstr, AbaxFixString &str );
	void makeRangeDataString( const JagSchemaAttribute *attrs[], const char *buffers[], 
						 const AbaxDataString &colobjstr, AbaxFixString &str );
    void getPolyDataString( JagMergeReaderBase *ntr, const AbaxDataString &polyType, const JagHashStrInt *maps[],
							 const JagSchemaAttribute *attrs[],
	                         const char *buffers[], AbaxFixString &str );
    void getPolyData( const AbaxDataString &polyType, JagMergeReaderBase *ntr, const JagHashStrInt *maps[], 
							 const JagSchemaAttribute *attrs[],
	                         const char *buffers[], AbaxFixString &str, bool is3D );
    void savePolyData( const AbaxDataString &polyType, JagMergeReaderBase *ntr, const JagHashStrInt *maps[], 
							 const JagSchemaAttribute *attrs[],
	                         const char *buffers[], const AbaxDataString &uuid, const AbaxDataString &db, const AbaxDataString &tab, 
							 const AbaxDataString &col, bool isBoundBox3D, bool is3D=false );

	AbaxDataString		_columns;
	AbaxFixString		_value;
	unsigned int		_tabnum;
	int					_typeMode;
	int					_nodenum; // a number set to distinguish each node of the tree

	unsigned int    _begincol;
	unsigned int    _endcol;
	unsigned int	_offset;
	unsigned int	_length;
	unsigned int	_sig;

	// AbaxDataString  _rowUUID;

};  // end StringElementNode

class BinaryOpNode: public ExpressionElementNode
{
  public:
	BinaryOpNode( BinaryExpressionBuilder* builder, short op, ExpressionElementNode *l, ExpressionElementNode *r, 
					    const JagParseAttribute &jpa, int arg1=0, int arg2=0, AbaxDataString carg1="" );
	virtual ~BinaryOpNode() {}
	BinaryOpNode &operator=(const BinaryOpNode& n) {}
	virtual int getBinaryOp() { return _binaryOp; }
	virtual int getName( const char *&p ) { return -1; }
	virtual bool getValue( const char *&p ) { return 0; }
	virtual void clear();
	virtual void print( int mode );

	// for select/where/on tree use
	virtual int setWhereRange( const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
								const int keylen[], const int numKeys[], int numTabs, bool &hasValue, 
								JagMinMax *minmaxbuf, AbaxFixString &str, int &typeMode, int &tabnum );
	virtual int setFuncAttribute( const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
									int &constMode, int &typeMode, bool &isAggregate, AbaxDataString &type, 
									int &collen, int &siglen );
	virtual int getFuncAggregate( JagVector<AbaxDataString> &selectParts, JagVector<int> &selectPartsOpcode, 
									JagVector<int> &selColSetAggParts, JagHashMap<AbaxInt, AbaxInt> &selColToselParts, 
									int &nodenum, int treenum );
	virtual int getAggregateParts( AbaxDataString &parts, int &nodenum );
	virtual int setAggregateValue( int nodenum, const char *buf, int length );
	virtual int checkFuncValid(JagMergeReaderBase *ntr, const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[],
							    const char *buffers[], AbaxFixString &str, int &typeMode, AbaxDataString &type, 
								int &length, bool &first, bool useZero, bool setGlobal );
	virtual int checkFuncValidConstantOnly( AbaxFixString &str, int &typeMode, AbaxDataString &type, int &length );
	static AbaxDataString binaryOpStr( short binaryOp );
	static bool isAggregateOp( short op );
	static bool isMathOp( short op );
	static bool isCompareOp( short op );
	static bool isStringOp( short op );
	static bool isSpecialOp( short op );
	static bool isTimedateOp( short op );
	static short getFuncLength( short op );

	// data members
	short					_binaryOp;
	int						_arg1; // use for substr and datediff (for now)
	int						_arg2; // use for substr and datediff (for now)
	AbaxDataString			_carg1; // use for substr and datediff (for now)
	ExpressionElementNode	*_left;
	ExpressionElementNode	*_right;
	int						_nodenum; // a number set to distinguish each node of the tree
	
  protected:
	// current class object use only
	void findOrBuffer( JagMinMax *minmaxbuf, JagMinMax *leftbuf, JagMinMax *rightbuf, 
						const int keylen[], const int numTabs );
	void findAndBuffer( JagMinMax *minmaxbuf, JagMinMax *leftbuf, JagMinMax *rightbuf, 
						const JagSchemaAttribute *attrs[], const int numTabs, const int numKeys[] );
	bool formatColumnData( JagMinMax *minmaxbuf, JagMinMax *iminmaxbuf, const AbaxFixString &value, int tabnum, int minOrMax );
	bool checkAggregateValid( int lcmode, int rcmode, bool laggr, bool raggr );
	int formatAggregateParts( AbaxDataString &parts, AbaxDataString &lparts, AbaxDataString &rparts );
	int _doWhereCalc( const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
						const int keylen[], const int numKeys[], int numTabs, int ltmode, int rtmode, int ltabnum, int rtabnum,
						JagMinMax *minmaxbuf, JagMinMax *lminmaxbuf, JagMinMax *rminmaxbuf, 
						AbaxFixString &str, AbaxFixString &lstr, AbaxFixString &rstr );
	int _doCalculation( AbaxFixString &lstr, AbaxFixString &rstr, 
						int &ltmode, int &rtmode, const AbaxDataString& ltype,  const AbaxDataString& rtype, 
						int llength, int rlength, bool &first );	

	bool processBooleanOp( int op, const AbaxFixString &lstr, const AbaxFixString &rstr, const AbaxDataString &carg );
	bool processStringOp( int op, const AbaxFixString &lstr, const AbaxFixString &rstr, const AbaxDataString &carg, AbaxDataString &res );
	bool processSingleDoubleOp( int op, const AbaxFixString &lstr, const AbaxDataString &carg, double &val );
	bool processSingleStrOp( int op, const AbaxFixString &lstr, const AbaxDataString &carg, AbaxDataString &val );
	bool doBooleanOp( int op, const AbaxDataString& mark1, const AbaxDataString &colType1, int srid1, const JagStrSplit &sp1,
							 const AbaxDataString& mark2, const AbaxDataString &colType2, int srid2, 
							 const JagStrSplit &sp2, const AbaxDataString &carg );
	bool doStringOp( int op, const AbaxDataString& mark1, const AbaxDataString &colType1, int srid1, const JagStrSplit &sp1,
							 const AbaxDataString& mark2, const AbaxDataString &colType2, int srid2, 
							 const JagStrSplit &sp2, const AbaxDataString &carg, AbaxDataString &res );
	bool doSingleDoubleOp( int op, const AbaxDataString& mark1, const AbaxDataString &colType1, int srid1, const JagStrSplit &sp1,
							 const AbaxDataString &carg, double &val );
	bool doSingleStrOp( int op, const AbaxDataString& mark1, const AbaxDataString& hdr, const AbaxDataString &colType1, int srid1, 
						const JagStrSplit &sp1, const AbaxDataString &carg, AbaxDataString &val );

	bool doAllWithin( const AbaxDataString& mark1, const AbaxDataString &colType1, int srid1, const JagStrSplit &sp1,
							 const AbaxDataString& mark2, const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, 
							 bool strict=true );
	bool doAllSame( const AbaxDataString& mark1, const AbaxDataString &colType1, int srid1, const JagStrSplit &sp1,
							 const AbaxDataString& mark2, const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2 );
	bool doAllClosestPoint( const AbaxDataString& mark1, const AbaxDataString &colType1, int srid1, const JagStrSplit &sp1,
							 const AbaxDataString& mark2, const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, 
							 AbaxDataString &res );
	bool doAllAngle2D( const AbaxDataString& mark1, const AbaxDataString &colType1, int srid1, const JagStrSplit &sp1,
							 const AbaxDataString& mark2, const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, 
							 AbaxDataString &res );
	bool doAllAngle3D( const AbaxDataString& mark1, const AbaxDataString &colType1, int srid1, const JagStrSplit &sp1,
							 const AbaxDataString& mark2, const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, 
							 AbaxDataString &res );
	bool doAllIntersect( const AbaxDataString& mark1, const AbaxDataString &colType1, int srid1, const JagStrSplit &sp1,
							 const AbaxDataString& mark2, const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, 
							 bool strict=true );
	bool doAllNearby( const AbaxDataString& mark1, const AbaxDataString &colType1, int srid1, const JagStrSplit &sp1,
							 const AbaxDataString& mark2, const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, 
							 const AbaxDataString &carg );
	bool doAllArea( const AbaxDataString& mk1, const AbaxDataString &colType1, int srid1, const JagStrSplit &sp1, double &val );
	bool doAllPerimeter( const AbaxDataString& mk1, const AbaxDataString &colType1, int srid1, const JagStrSplit &sp1, double &val );
	bool doAllVolume( const AbaxDataString& mk1, const AbaxDataString &colType1, int srid1, const JagStrSplit &sp1, double &val );
	bool doAllMinMax( int op, const AbaxDataString& mk1, const AbaxDataString &colType1, const JagStrSplit &sp1, double &val );
	bool doAllPointN( const AbaxDataString& mk1, const AbaxDataString &colType1, int srid1, const JagStrSplit &sp1, 
					  const AbaxDataString &carg, AbaxDataString &val );
	bool doAllBBox( const AbaxDataString& mk1, const AbaxDataString &colType1, const JagStrSplit &sp1, AbaxDataString &val );
	bool doAllStartPoint( const AbaxDataString& mk1, const AbaxDataString &colType1, const JagStrSplit &sp1, AbaxDataString &val );
	bool doAllEndPoint( const AbaxDataString& mk1, const AbaxDataString &colType1, const JagStrSplit &sp1, AbaxDataString &val );
	bool doAllConvexHull( const AbaxDataString& mk1, const AbaxDataString& hdr, const AbaxDataString &colType1, 
						  int srid, const JagStrSplit &sp1, AbaxDataString &val );
	bool doAllBuffer( const AbaxDataString& mk1, const AbaxDataString& hdr, const AbaxDataString &colType1, 
						  int srid, const JagStrSplit &sp1, const AbaxDataString& carg, AbaxDataString &val );
	bool doAllCentroid( const AbaxDataString& mk1, const AbaxDataString& hdr, const AbaxDataString &colType1, 
						  int srid, const JagStrSplit &sp1, AbaxDataString &val );
	bool doAllIsClosed( const AbaxDataString& mk1, const AbaxDataString &colType1, const JagStrSplit &sp1, AbaxDataString &val );
	bool doAllNumPoints( const AbaxDataString& mk1, const AbaxDataString &colType1, const JagStrSplit &sp1, AbaxDataString &val );
	bool doAllNumSegments( const AbaxDataString& mk1, const AbaxDataString &colType1, const JagStrSplit &sp1, AbaxDataString &val );
	bool doAllNumRings( const AbaxDataString& mk1, const AbaxDataString &colType1, const JagStrSplit &sp1, AbaxDataString &val );
	bool doAllSummary( const AbaxDataString& mk, const AbaxDataString &colType, int srid, const JagStrSplit &sp, AbaxDataString &val );


	// data members
	AbaxFixString 	_opString;
	abaxint 		_numCnts; // use for calcuations
	abaxdouble 		_initK; // use for stddev
	abaxdouble 		_stddevSum; //use for stddev
	abaxdouble 		_stddevSumSqr; // use for stddev
	std::regex      *_reg;

};

class BinaryExpressionBuilder
{
  public:
	BinaryExpressionBuilder();
	~BinaryExpressionBuilder();
	void init( const JagParseAttribute &jpa, JagParseParam *pparam );
	void clean();

	BinaryOpNode *parse( const JagParser *jagParser, const char* str, int type,
								const JagHashMap<AbaxString, AbaxPair<AbaxString, abaxint>> &cmap, JagHashStrInt &jmap, 
								AbaxDataString &colList );
	ExpressionElementNode *getRoot() const;
	
	JagParseAttribute _jpa;
	JagParseParam *_pparam;

  private:
	// holds either (, +, -, *, /, %, ^ or any other function type  
	JagStack<int> operatorStack;	

	// operandStack is made up of BinaryOpNodes and StringElementNode
	JagStack<ExpressionElementNode*> operandStack; 
	int 		_datediffClause;
	int			_substrClause;
	int  		_lastOp;
	bool 		_isNot;
	bool 		_lastIsOperand;
	
	// methods
	void processBetween( const JagParser *jpars, const char *&p, const char *&q, StringElementNode &lastNode,
						const JagHashMap<AbaxString, AbaxPair<AbaxString, abaxint>> &cmap, JagHashStrInt &jmap, 
						AbaxDataString &colList );
	void processIn( const JagParser *jpars, const char *&p, const char *&q, StringElementNode &lastNode,
					const JagHashMap<AbaxString, AbaxPair<AbaxString, abaxint>> &cmap, JagHashStrInt &jmap, 
					AbaxDataString &colList );
	void processOperand( const JagParser *jpars, const char *&p, const char *&q, StringElementNode &lastNode,
						const JagHashMap<AbaxString, AbaxPair<AbaxString, abaxint>> &cmap, AbaxDataString &colList );

	void processOperator( short op, JagHashStrInt &jmap );
	void processRightParenthesis( JagHashStrInt &jmap );
	void doBinary( short op, JagHashStrInt &jmap );
	short precedence( short fop );
	bool funcHasZeroChildren( short fop );
	bool funcHasOneChildren( short fop );
	bool funcHasTwoChildren( short fop );
	bool funcHasThreeChildren( short fop );
	bool checkFuncType( short fop );
	bool getCalculationType( const char *p, short &fop, short &len, short &ctype );
	bool nameConvertionCheck( AbaxDataString &name, int &tabnum,
								const JagHashMap<AbaxString, AbaxPair<AbaxString, abaxint>> &cmap, 
								AbaxDataString &colList );
    bool nameAndOpGood( const JagParser *jpsr, const AbaxDataString &fullname, const StringElementNode &lastNode );
};

#endif
