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

#include <JagUUID.h>
#include <JagNet.h>

#ifdef _WINDOWS64_
#include <winsock2.h>
#include <windows.h>
#endif

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <math.h>
#include <time.h>
#include <abax.h>

JagUUID::JagUUID()
{
	JagNet::socketStartup();
	_hostID = JagNet::getMacAddress();
	_pid = getpid();
	srand( _pid%100);
	_getHostStr();
	_getPidStr();
}

Jstr JagUUID::getString()
{
	char ds[41];
	long n = time(NULL)%100000;
	sprintf(ds, "%s%s%s%05d", AbaxString::randomValue(27).c_str(), _hostStr.c_str(), _pidStr.c_str(), n );
	// 27+5+3+5=40 bytes
	return ds;
}

long JagUUID::getLong(long M)
{
	return time(NULL)%M + rand()%M;
}



// 5 bytes short string
void JagUUID::_getHostStr()
{
	static char cset[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"; // 62 total
	AbaxString xs = _hostID;
	char c5[6];
	abaxint hc = xs.hashCode();
	int  d5 = hc%100000;  // 87213
	c5[0] = cset[ d5/10000];
	c5[1] = cset[ (d5/1000)%10];
	c5[2] = cset[ (d5/100)%10];
	c5[3] = cset[ (d5/10)%10];
	c5[4] = cset[ d5%10];
	c5[5] = '\0';
	_hostStr = c5;

	// printf("_hostStr=[%s] _hostName=[%s]\n", _hostStr.c_str(), _hostName.c_str()  );
}

// 3 bytes short string
void JagUUID::_getPidStr()
{
	static char cset[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"; // 62 total
	char p4[4];
	int pid = _pid % 1000;  // 382
	p4[0] = cset[ pid/100];
	p4[1] = cset[ (pid/10)%10];
	p4[2] = cset[ pid%10];
	p4[3] = '\0';
	_pidStr = p4;
}
