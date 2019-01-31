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
#ifndef _adb_h_
#define _adb_h_

#include <JagDef.h>

class ADBROW
{
  public:
	int    retCode;
	bool   hasSchema;
	char   *data;
	char   *hdr;
	abaxint   datalen;
	char   type;
	char 	*g_names[JAG_COL_MAX];
	char 	*g_values[JAG_COL_MAX];
	jag_hash_t  colHash;
	int		isMeta;
	int		colCount;
	void   *etc;
	JagKeyValProp prop[JAG_COL_MAX];
	int    numKeyVals;
};

class JagParseParam;

class CliPass
{
  public:
    CliPass() { 
		syncDataCenter = 0; needAll = 0; hasError = 0; isContinue = 1; recvJoinEnd = 0; 
		joinEachCnt = NULL; parseParam = NULL;
		isToGate = 0; 
		noQueryButReply = 0;
		redirectSock = INVALID_SOCKET;
	}
	
	bool hasLimit;
	bool hasError;
	bool needAll;
	bool syncDataCenter;
	bool isToGate;
	bool noQueryButReply;
	int idx;
	int passMode; // 0: ignore reply; 1: print one reply; 2: handshake NG/OK and no reply; 3: handshake NG/OK and one reply
	int selectMode; // 0: update/delete; 1: select point query; 2: select range query; 3: select count(*) query
	int isContinue;
	int recvJoinEnd;
	JAGSOCK redirectSock;
	abaxint totnum;
	abaxint start;
	abaxint cnt;
	Jstr cmd;
	JaguarCPPClient *cli;
	Jstr errmsg;
	JagParseParam *parseParam;
	int *joinEachCnt;
};

class RecoverPass
{
  public:
	RecoverPass() { pcli = NULL; result = 0; }
	Jstr fpath;
	JaguarCPPClient *pcli;
	int result;
};

#endif
