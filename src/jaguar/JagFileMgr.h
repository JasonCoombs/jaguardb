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
#ifndef _file_mgr_h_
#define _file_mgr_h_

#include <sys/stat.h>
#include <unistd.h>
#include <abax.h>

class JagFileMgr
{
	public:

		static void 	makedirPath( const AbaxDataString &fullpath, int mode=0755 );
		static void 	rmdir( const AbaxDataString &fullpath, bool rmtop=true );
		static abaxint 	fileSize( const AbaxDataString &fullpath );
		static int 		getPathUsage( const char *fpath,  abaxint &usedGB, abaxint &freeGB );
		static int 		isFile( const AbaxDataString &fullpath );
		static int 		isDir( const AbaxDataString &fullpath );
		// static size_t 	dirSize( const AbaxDataString &fullpath );
		static bool 	dirEmpty( const AbaxDataString &fullPathDir );
		static int 		exist( const AbaxDataString &fullpath );
		static void 	readTextFile(const AbaxDataString &fname, AbaxDataString &content );
		static void 	writeTextFile(const AbaxDataString &fname, const AbaxDataString &content );
		static void 	writeTextFile(const AbaxDataString &fname, const char *content );
		static int 		cleanDir( const AbaxDataString &fullpath, time_t historySeconds );
		static int 		cleanDirExclude( const AbaxDataString &fullpath, const AbaxDataString &exsuffix );
		static abaxint 	numObjects( const AbaxDataString &fullpath );
		static AbaxDataString 	listObjects( const AbaxDataString &fullpath );
		static AbaxDataString     makeLocalLogDir( const AbaxDataString &subdir="" );
		static AbaxDataString     getLocalLogDir( const AbaxDataString &subdir="" );
		static int 		getIOStat( uabaxint &reads, uabaxint & writes );
		static    int   fallocate(int fd, abaxint offset, abaxint len);
		static AbaxDataString getFileFamily( const AbaxDataString &fullpath, const AbaxDataString &objname, bool fnameOnly=false );
		static    int   pathWritable( const AbaxDataString &fullpath );


};

#endif
