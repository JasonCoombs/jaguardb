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
#ifndef _jag_table_h_
#define _jag_table_h_

#include <stdio.h>
#include <string.h>
#include <atomic>
#include <JagDiskArrayServer.h>

#include <JagSchemaRecord.h>
#include <JagTableSchema.h>
#include <JagIndexSchema.h>
#include <JagDBPair.h>
#include <JagUtil.h>
#include <JagSchemaRecord.h>
#include <JagDBServer.h>
#include <JagColumn.h>
#include <JagVector.h>
#include <JagParseExpr.h>
#include <JagTableUtil.h>
#include <JagMergeReader.h>
#include <JagMergeBackReader.h>
#include <JagMemDiskSortArray.h>
#include <JagDiskArrayFamily.h>

class JagDataAggregate;
class JagIndexString;
class JagIndex;

struct JagPolyPass
{
	double xmin, ymin, zmin, xmax, ymax, zmax;
	int tzdiff, srvtmdiff;
	int getxmin, getymin, getzmin, getxmax, getymax, getzmax;
	int getid, getcol, getm, getn, geti, getx, gety, getz; 
	bool is3D;
	Jstr colname, lsuuid;
	int  col, m, n, i;
};

class JagTable
{

  public:
 
  	JagTable( int replicateType, const JagDBServer *servobj, const Jstr &dbname, const Jstr &tableName, 
				const JagSchemaRecord &record, bool buildInitIndex=true );
  	~JagTable();
	inline int getNumIndexes() { return _indexlist.size(); }
	inline int getnumCols() { return _numCols; }
	inline int getnumKeys() { return _numKeys; }	
	inline Jstr getdbName() { return _dbname; }
	inline Jstr getTableName() { return _tableName; }
	inline JagHashStrInt *getTableMap() { return _tablemap; }
	inline JagSchemaAttribute *getSchemaAttributes() { return _schAttr; }
	const JagSchemaRecord *getRecord() { return &_tableRecord; }
	inline void debugObject( int flag, abaxint limit, abaxint hold, abaxint instart, abaxint inend ) 
			{ _darrFamily->debugJDBFile( flag, limit, hold, instart, inend ); }
	bool getPair( JagDBPair &pair );
	void getlimitStart( abaxint &startlen, abaxint limitstart, abaxint& soffset, abaxint &foffset ); 
	int insert( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg, int &insertCode, bool direct );
	int sinsert( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg, int &insertCode, bool direct );
	int cinsert( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg, int &insertCode, bool direct );
	int dinsert( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg, int &insertCode, bool direct );
	// int upsert( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg, int &insertCode, bool direct );
	int parsePair( int tzdiff, JagParseParam *parseParam, JagVector<JagDBPair> &pairVec, Jstr &errmsg );
	int insertPair( JagDBPair &pair, int &insertCode, bool direct, int mode ); 
	Jstr drop( Jstr &errmsg, bool isTruncate=false );
	int renameIndexColumn ( const JagParseParam *parseParam, Jstr &errmsg );
	int setIndexColumn ( const JagParseParam *parseParam, Jstr &errmsg );
	void dropFromIndexList( const Jstr &indexName );
	void buildInitIndexlist();
	void setGetFileAttributes( const Jstr &hdir, JagParseParam *parseParam, const char *buffers[] );

	abaxint update( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg, int &insertCode );
	abaxint remove( const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg );	
	abaxint count( const char *cmd, const JagRequest &req, JagParseParam *parseParam, Jstr &errmsg, abaxint &keyCheckerCnt );
	abaxint select( JagDataAggregate *&jda, const char *cmd, const JagRequest &req, JagParseParam *parseParam, 
					Jstr &errmsg, bool nowherecnt=true, bool isInsertSelect=false );
	
	static void *parallelSelectStatic( void * ptr );
	static int buildDiskArrayForGroupBy( JagMergeReaderBase *ntr, const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
										 const JagRequest &req, const char *buffers[], 
										JagParseParam *parseParam, JagMemDiskSortArray *gmdarr, char *gbvbuf );
	static void groupByFinalCalculation( char *gbvbuf, bool nowherecnt, abaxint finalsendlen, std::atomic<abaxint> &cnt, abaxint actlimit, 
										const JagRequest &req, const Jstr &writeName, JagParseParam *parseParam, 
										JagDataAggregate *jda, JagMemDiskSortArray *gmdarr, const JagSchemaRecord *nrec );
	static void nonAggregateFinalbuf( JagMergeReaderBase *ntr, const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
										const JagRequest &req, const char *buffers[], JagParseParam *parseParam, char *finalbuf, abaxint finalsendlen,
										JagDataAggregate *jda, const Jstr &writeName, std::atomic<abaxint> &cnt, bool nowherecnt, 
										const JagSchemaRecord *nrec, bool oneLine );
	static void aggregateDataFormat( JagMergeReaderBase *ntr,  const JagHashStrInt *maps[], const JagSchemaAttribute *attrs[], 
									 const JagRequest &req, const char *buffers[], JagParseParam *parseParam, bool init );
	static void aggregateFinalbuf( const JagRequest &req, const Jstr &sendhdr, abaxint len, JagParseParam *parseParam[], 
		char *finalbuf, abaxint finalsendlen, JagDataAggregate *jda, const Jstr &writeName, 
		std::atomic<abaxint> &cnt, bool nowherecnt, const JagSchemaRecord *nrec );
	static void doWriteIt( JagDataAggregate *jda, const JagParseParam *parseParam, const JagRequest &req,
		const Jstr &host, const char *buf, abaxint buflen, const JagSchemaRecord *nrec=NULL );
	static void formatInsertFromSelect( const JagParseParam *parseParam, const JagSchemaAttribute *attrs, 
		const char *finalbuf, const char *buffers, abaxint finalsendlen, abaxint numCols,
		JaguarCPPClient *pcli, std::atomic<abaxint> &cnt, const Jstr &iscmd );

	int formatIndexCmd( JagDBPair &pair, int mode );
	int formatCreateIndex( JagIndex *pindex );
	static void *parallelCreateIndexStatic( void * ptr );

	static abaxint sendMessage( const JagRequest &req, const char *mesg, const char *type );
	static abaxint sendMessageLength( const JagRequest &req, const char *mesg, abaxint len, const char *type );
	static abaxint sendMessageLength2( JagSession *session, const char *mesg, abaxint len, const char *type );

	/**** hit invalid fastbin entry bug
	static void fillCmdParse( int objType, void *objPtr, int i, abaxint gbvsendlen, JagParseParam *pparam[], 
							 JagMemDiskSortArray *lgmdarr, const JagRequest &req, JagDataAggregate *jda, 
							 const Jstr &dbobj, std::atomic<abaxint> *pcnt, abaxint nm, 
							 bool nowherecnt, JagSchemaRecord *prec, abaxint memlim, 
							 JagMinMax minmaxbuf[], 
							 abaxint bsec, abaxint KEYVALLEN, const JagDBServer *servobj, abaxint numthrds,
							 const JagDiskArrayFamily *darrFamily,
							 bool lcpu,
							 ParallelCmdPass psp[]  );
	 *******/



	void   flushBlockIndexToDisk();
	void   copyAndInsertBufferAndClean( JagIndex *ipindex=NULL );
	Jstr getIndexNameList();
	int refreshSchema();
	void setupSchemaMapAttr( int numCols );
	bool hasSpareColumn();

	static const abaxint keySchemaLen = JAG_SCHEMA_KEYLEN;
    static const abaxint valSchemaLen = JAG_SCHEMA_VALLEN;
  	static const int _THREAD_NUM = 4;
	std::atomic<abaxint>					_actualInsert;
	std::atomic<int>					_isExporting;
	Jstr 						_dbtable;
	abaxint 								KEYLEN;
	abaxint 								VALLEN;
	abaxint 								KEYVALLEN;
	int									_replicateType;
	JagSchemaRecord 					_tableRecord;
	JagDiskArrayFamily						*_darrFamily;

	int	    							_numCols;
	int 								_numKeys;
	JagHashStrInt 					 	*_tablemap;
	JagSchemaAttribute					*_schAttr;
	JagVector<int>				_defvallist;
	JagVector<Jstr>	_indexlist;
	const JagDBServer			*_servobj;

	JagTableSchema				*_tableschema;
	JagIndexSchema				*_indexschema;
	
  protected:
	bool						_finishInsertMerge;
	abaxint						_bufferRows;
	time_t  					_schemaRefreshTime;
	time_t  					_indexRefreshTime;
	const JagCfg  				*_cfg;
	Jstr  			_dbname;
	Jstr 				_tableName;
	JagBlockLock				*_mergeLock;
	int  						_objectType;

	//  colname-->position in schema (also in table record)
	JagVector<JagDBPair> 				_removeCache;
	JagHashArray<JagDBPair>		 		*_cacheSelect;

	
	void init( bool buildInitIndex );
	void destroy();
	int refreshIndexList();
	int _removeIndexRecords( const char *buf );
	int  removeColFiles(const char *kvbuf );
	bool isFileColumn( const Jstr &colname );
	void formatPointsInLineString( const JagLineString &line, char *tablekvbuf, const JagPolyPass &pass, 
								   JagVector<JagDBPair> &retpair, Jstr &errmg );
	void getColumnIndex( const Jstr &dbtab, const Jstr &colname, bool is3D,
                         int &getx, int &gety, int &getz, int &getxmin, int &getymin, int &getzmin,
                         int &getxmax, int &getymax, int &getzmax,
                         int &getid, int &getcol, int &getm, int &getn, int &geti );


};

typedef JagTable* JagTablePointer;


/********** Still hit invaid fastbin entry bug
#define fillCmdParse( objType, objPtr, i, gbvsendlen, pparam, notnull, lgmdarr, req, jda, dbobj, cnt, nm, nowherecnt, rec, memlim, minmaxbuf, bsec, KEYVALLEN, servobj, numthrds, darrFamily, lcpu )   \
	if ( JAG_TABLE == objType ) { \
		psp[i].ptab = (JagTable*)objPtr; \
		psp[i].pindex = NULL; \
	} else { \
		psp[i].ptab = NULL; \
		psp[i].pindex = (JagIndex*)objPtr; \
	} \
	psp[i].pos = i; \
	psp[i].sendlen = gbvsendlen; \
	psp[i].parseParam = pparam[i]; \
	if ( notnull ) psp[i].gmdarr = lgmdarr[i]; else psp[i].gmdarr = NULL; \
	psp[i].req = req; \
	psp[i].jda = jda; \
	psp[i].writeName = dbobj; \
	psp[i].cnt = &cnt; \
	psp[i].nrec = &rec; \
	psp[i].actlimit = nm; \
	psp[i].nowherecnt = nowherecnt; \
	psp[i].memlimit = memlim; \
	psp[i].minbuf = minmaxbuf[0].minbuf; \
	psp[i].maxbuf = minmaxbuf[0].maxbuf; \
	psp[i].starttime = bsec; \
	psp[i].kvlen = KEYVALLEN; \
	if ( lcpu ) { \
		psp[i].spos = i; \
		psp[i].epos = i; \
	} else { \
		psp[i].spos = i*servobj->_numCPUs; \
		psp[i].epos = psp[i].spos+servobj->_numCPUs-1; \
		if ( i == numthrds-1 ) psp[i].epos = darrFamily->_darrlistlen-1; \
	} 

***************/

#endif
