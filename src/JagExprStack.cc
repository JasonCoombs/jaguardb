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
#include <JagExprStack.h>
#include <JagParseExpr.h>

// ctor
JagExprStack::JagExprStack( int initSize )
{
	_arr = new ExprElementNode*[initSize];
	_arrlen = initSize;
	_last = -1;
	_isDestroyed = false;

	numOperators = 0;
}

void JagExprStack::destroy( )
{
	if ( ! _arr || _isDestroyed ) { return; }
	ExprElementNode *p;
	while ( ! empty() ) {
		p = top();
		//prt(("s1093 stack obj p=%0x binaryop=%d\n", p, p->getBinaryOp() ));
		delete p;
		pop();
	}

	if ( _arr ) delete [] _arr;
	_arr = NULL;
	_arrlen = 0;
	_isDestroyed = true;
}


// dtor
JagExprStack::~JagExprStack( )
{
	destroy();
}

void JagExprStack::clean()
{
	//prt(("s8282 stack cleaned this=%0x\n", this ));
	destroy();
}

void JagExprStack::reAlloc()
{
	jagint i;
	jagint newarrlen  = _GEO*_arrlen; 
	ExprElementNode **newarr;

	newarr = new ExprElementNode *[newarrlen];
	for ( i = 0; i <= _last; ++i) {
		newarr[i] = _arr[i];
	}
	//if ( _arr ) free( _arr );
	if ( _arr ) delete [] _arr;
	_arr = newarr;
	newarr = NULL;
	_arrlen = newarrlen;
}

void JagExprStack::reAllocShrink()
{
	jagint i;
	ExprElementNode **newarr;

	jagint newarrlen  = _arrlen/_GEO; 
	//prt(("s1838 reAllocShrink newarrlen=%d _last=%d\n", newarrlen, _last ));
	newarr = new ExprElementNode*[newarrlen];
	for ( i = 0; i <= _last; ++i) {
		newarr[i] = _arr[i];
	}

	//if ( _arr ) free( _arr );
	if ( _arr ) delete [] _arr;
	_arr = newarr;
	newarr = NULL;
	_arrlen = newarrlen;
}

void JagExprStack::push( ExprElementNode *newnode )
{
	/***
	if ( newnode->_isElement ) {
		prt(("s9282 ****** pushed %0x iselement\n", newnode ));
	} else {
		prt(("s9282 ****** pushed %0x isbinopnode op=%d\n", newnode, newnode->getBinaryOp() ));
	} 
	***/

	if ( NULL == _arr ) {
		_arr = new ExprElementNode*[4];
		_arrlen = 4;
		_last = -1;
	}

	if ( _last == _arrlen-1 ) { reAlloc(); }

	++ _last;
	_arr[_last] = newnode;

	if ( ! newnode->_isElement ) { ++ numOperators; }
}

// back: add end (enqueue end)
ExprElementNode* JagExprStack::top() const
{
	if ( _last < 0 ) {
		//prt(("s5004 stack empty, error top()\n"));
		throw 2920;
	} 
	return _arr[ _last ];
}


void JagExprStack::pop()
{
	if ( _last < 0 ) { return; } 

	/***
	if ( _arrlen >= 64 ) {
    	jagint loadfactor  = (100 * _last) / _arrlen;
    	if (  loadfactor < 20 ) {
    		reAllocShrink();
    	}
	} 
	***/
	//prt(("s9283 popped %0x isElement=%d\n", _arr[_last], _arr[_last]->_isElement ));

	/***
	if ( _arr[_last]->_isElement ) {
		prt(("s9283 ****** popped %0x iselement\n", _arr[_last] ));
	} else {
		prt(("s9283 ****** popped %0x isbinopnode\n", _arr[_last] ));
	} 
	***/

	if ( ! _arr[_last]->_isElement ) { 
		-- numOperators; 
	}
	-- _last;
}


void JagExprStack::print()
{
	jagint i;
	printf("c3012 JagExprStack this=%0x _arrlen=%d _last=%d \n", this, _arrlen, _last );
	for ( i = 0; i  <= _last; ++i) {
		printf("%09d  %0x\n", i, _arr[i] );
	}
	printf("\n");
}

int JagExprStack::lastOp() const
{
	int op = -1;
	for ( int i = _last; i >=0; --i ) {
		if ( ! _arr[i]->_isElement ) {
			op =  _arr[i]->getBinaryOp();
			break;
		}
	}
	return op;
}


const ExprElementNode* JagExprStack::operator[](int i) const 
{ 
	if ( i<0 ) { i=0; }
	else if ( i > _last ) { i = _last; }
	return _arr[i];
}

ExprElementNode*& JagExprStack::operator[](int i) 
{ 
	if ( i<0 ) { i=0; }
	else if ( i > _last ) { i = _last; }
	return _arr[i];
}
