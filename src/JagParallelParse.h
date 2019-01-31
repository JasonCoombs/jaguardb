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
#ifndef _JagParallelParse_h_
#define _JagParallelParse_h_

#include <pthread.h>

class JaguarThreadPool;
class JagParseParam;
class JagParseAttribute;
class JagStrSplitWithQuote;

class JagParallelParse
{
  public:
  	JagParallelParse( int numCPUS, JaguarThreadPool *pool );
	~JagParallelParse();

	int parse( const JagParseAttribute &jpa, const JagStrSplitWithQuote &split, JagParseParam *param[] );
	int parseSame( const JagParseAttribute &jpa, const char *cmd, abaxint len, JagParseParam *param[] );
	pthread_mutex_t  _mutex;
	pthread_cond_t  _cond;
	abaxint   _numDone;

  protected:
  	JaguarThreadPool *_pool;
	int   _numCPUs;

};

class ParallelPass
{
  public:
  	Jstr  sql;
	JagParseParam  **pparam;
	JagParallelParse  *obj;
	JagParseAttribute  *jpa;
};


void *parseSQLFunction( void *p );

#endif
