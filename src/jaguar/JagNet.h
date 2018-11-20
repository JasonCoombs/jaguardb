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
#ifndef _jag_net_h_
#define _jag_net_h_

#include <JagGlobalDef.h>

//////// if
#ifdef _WINDOWS64_

#ifdef _WIN32_WINNT
#undef _WIN32_WINNT
#endif
#define _WIN32_WINNT 0x0602

#include <winsock2.h>
#include <mswsock.h>
#include <ws2tcpip.h>
#include <ws2def.h>
#include <mstcpip.h>
#include <IPIfCons.h>
#include <windows.h>
#include <iphlpapi.h>
#include <stdlib.h>
#include <stdint.h>
#pragma comment(lib,"ws2_32.lib") //Winsock Library
#pragma comment(lib, "IPHLPAPI.lib")
typedef SOCKET JAGSOCK;
typedef const char * CHARPTR;

///////// else Linux
#else
#define INVALID_SOCKET -1
typedef int JAGSOCK;
typedef void * CHARPTR;
#include <ifaddrs.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <netinet/in.h>
#include <poll.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <linux/netdevice.h>
///////// end
#endif

#ifdef _WINDLL
#define snprintf _snprintf
#else
#include <sys/time.h>
#include <unistd.h>
#endif

#ifdef _WINDOWS64_
#define socket_bad( sock ) ( sock == INVALID_SOCKET )
#else 
#define socket_bad( sock ) ( sock <= INVALID_SOCKET )
#endif

#include <abax.h>
#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>
#include <JagStrSplit.h>
#include <JagVector.h>


JAGSOCK rayconnect( const char *host, unsigned int port, int timeoutsecs,  bool errPrint=true );
void rayclose ( JAGSOCK winsock );
int connectWithTimeout(JAGSOCK sockfd, const struct sockaddr *addr, socklen_t addrlen, int timeoutmillisec );
void beginBulkSend( JAGSOCK sock );
void endBulkSend( JAGSOCK sock );

class JagNet
{
  public:
	static AbaxDataString getLocalHost();
	static void getLocalIPs( JagVector<AbaxDataString> &vec );
	static AbaxDataString getMacAddress();
	static JAGSOCK connect2Host( const char *host, short port, int timeoutsecs, bool errPrint );
	static int getNetStat( uabaxint &reads, uabaxint &writes );
	static JAGSOCK createIPV4Socket( const char *ip, short port );
	static void socketNonBlocking( JAGSOCK sock );
	static void socketBlocking( JAGSOCK sock );
	static int socketStartup();
	static int socketCleanup();
	static int  _socketHasBeenSetup;
	static int getNumTCPConnections();
	static void  setRecvSndTimeOut( JAGSOCK sock, int dtimeout, int cliservSameProcess );
	static void getIPFromHostName( const char *hostname, char *ip ); 
	static AbaxDataString getIPFromHostName( const AbaxDataString &hostname );
	static void getIPFromEtcFile( const char *hostname, char *ip );
	// static JAGSOCK acceptTimeout( JAGSOCK sock, struct sockaddr *pcliaddr, socklen_t* pclilen, abaxint thrdGroup, int timeOutseconds );

};

#endif
