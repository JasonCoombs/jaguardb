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
#ifndef _raydb_connector_h_
#define _raydb_connector_h_

#include <abax.h>
#include <JagHashMap.h>
#include <JagNodeMgr.h>
#include <jaghashtable.h>
#include <JagMutex.h>

class JaguarCPPClient;
class JagDBConnector;

class BroadCastAttr
{
  public:
	JaguarCPPClient *usecli;
	Jstr host;
	Jstr cmd;
	Jstr value;
	bool getStr;
	bool result;
};

class JagDBConnector
{
  public:
  	JagDBConnector();
  	~JagDBConnector();

	void makeInitConnection( bool debugClient );
	Jstr broadCastGet( const Jstr &signal, const Jstr &hostlist, JaguarCPPClient *ucli=NULL );
	bool broadCastSignal( const Jstr &signal, const Jstr &hostlist, JaguarCPPClient *ucli=NULL );
	static void *queryDBServerStatic( void *ptr );

	// data members
	short _port;
	Jstr _passwd;
	Jstr _servToken;
	JagNodeMgr *_nodeMgr;
	JaguarCPPClient *_parentCliNonRecover;
	JaguarCPPClient *_parentCli;
	JaguarCPPClient *_broadCastCli;
	Jstr   _listenIP;
	JagReadWriteLock *_lock;

};

#endif
