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

#define JAG_SERVER_SIDE 1

#include <abax.h>
#include <JagParallelParse.h>
#include <JagThreadPool.h>
#include <JagParseParam.h>
#include <JagParser.h>
#include <JagStrSplit.h>
#include <JagUtil.h>
#include <JagStrSplitWithQuote.h>

JagParallelParse::JagParallelParse( int numCPUs, JaguarThreadPool *pool )
{
	_pool = pool;
	_numCPUs = numCPUs;
	pthread_mutex_init(& _mutex, NULL);
	pthread_cond_init(& _cond,  NULL);
}

JagParallelParse::~JagParallelParse()
{
	pthread_mutex_destroy(& _mutex );
	pthread_cond_destroy(& _cond );
}

#if 0
int JagParallelParse::parse( const JagParseAttribute &jpa, const JagStrSplitWithQuote &split, JagParseParam *param[] )
{
	// get md5
	// addParse func to thread
	abaxint len = split.length();
	ParallelPass *pass[len];
	for ( int i = 0; i < len; ++i ) {
		pass[i] = new ParallelPass();
	}

	_numDone = 0;
	for ( abaxint i = 0; i < len; ++i ) {
		pass[i]->sql = split[i];
		pass[i]->pparam = & param[i];  // pointer 
		pass[i]->obj = this;
		pass[i]->jpa = (JagParseAttribute*)&jpa;
		_pool->addWork( parseSQLFunction, pass[i] );
	}

    jaguar_mutex_lock( &_mutex );
 	while ( _numDone < len ) {
	    jaguar_cond_wait( & _cond, & _mutex );
    }
    jaguar_mutex_unlock( &_mutex );
	for ( int i = 0; i < len; ++i ) {
		delete pass[i];
	}

	return 1;
}
#else
// output is param[]
// limit threads to 2*_numCPUS
int JagParallelParse::parse( const JagParseAttribute &jpa, const JagStrSplitWithQuote &split, JagParseParam *param[] )
{
	// addParse func to thread
	abaxint total = split.length();
	abaxint len = 2 * _numCPUs; 
   	ParallelPass *pass[len];
   	for ( int i = 0; i < len; ++i ) { pass[i] = newObject<ParallelPass>(); }

	abaxint k = 0 ;
	int done = 0;
	int donum = 0;
	while (  1 ) {
    	_numDone = 0;
		donum = 0;
    	for ( abaxint i = 0; i < len; ++i ) {
			if ( k >= total ) {
				done = 1;
				break;
			}

			++donum;
    		pass[i]->sql = split[k];
    		pass[i]->pparam = & param[k++];  // pointer 
    		pass[i]->obj = this;
    		pass[i]->jpa = (JagParseAttribute*)&jpa;
    		_pool->addWork( parseSQLFunction, pass[i] );
    	}
    
        jaguar_mutex_lock( &_mutex );
     	while ( _numDone < donum ) {
    	    jaguar_cond_wait( & _cond, & _mutex );
        }
        jaguar_mutex_unlock( &_mutex );

		if ( done ) { break; }
	}

   	for ( int i = 0; i < len; ++i ) {
		delete pass[i];
   	}

	return 1;
}

#endif

// limit threads to 2*_numCPUS
// copy from ::parse(), just cmd diff
int JagParallelParse::parseSame( const JagParseAttribute &jpa, const char *cmd, abaxint total, JagParseParam *param[] )
{
	abaxint len = 2 * _numCPUs; 
   	ParallelPass *pass[len];
   	for ( int i = 0; i < len; ++i ) { 
		pass[i] = newObject<ParallelPass>(); 
	}

	abaxint k = 0 ;
	int done = 0;
	int donum = 0;
	while (  1 ) {
    	_numDone = 0;
		donum = 0;
    	for ( abaxint i = 0; i < len; ++i ) {
			if ( k >= total ) {
				done = 1;
				break;
			}

			++donum;
			pass[i]->sql = cmd;
    		pass[i]->pparam = & param[k++];  // pointer 
    		pass[i]->obj = this;
    		pass[i]->jpa = (JagParseAttribute*)&jpa;
    		_pool->addWork( parseSQLFunction, pass[i] );
    	}
    
        jaguar_mutex_lock( &_mutex );
     	while ( _numDone < donum ) {
    	    jaguar_cond_wait( & _cond, & _mutex );
        }
        jaguar_mutex_unlock( &_mutex );

		if ( done ) { break; }
	}

   	for ( int i = 0; i < len; ++i ) {
		delete pass[i];
   	}

	return 1;
}


void *parseSQLFunction( void *p )
{
	ParallelPass *pass = (ParallelPass*)p;
	Jstr sql = pass->sql;
	JagParseParam **param = pass->pparam;
	JagParseAttribute *jpa = pass->jpa;
	bool rc;
	Jstr errmsg;

    jaguar_mutex_lock( & pass->obj->_mutex );
	// prt(("s3024 parseSQLFunction parseCommand(%s) ...\n", sql.c_str() ));
	JagParser parser((void*)NULL);
	(*param)->jagParser = &parser;

	rc = parser.parseCommand( *jpa, sql, *param, errmsg );
	++ pass->obj->_numDone;
	jaguar_cond_signal( & pass->obj->_cond );
    jaguar_mutex_unlock( & pass->obj->_mutex );
	return NULL;
}

