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
#ifndef _abaxmemory_h_
#define _abaxmemory_h_

// Memory Class
class JagMemory
{
    public:
    JagMemory()
    {
        endkb = beginkb = 0; 
    }

    void start()
    {
		beginkb = snapShot();
    }

    void stop()
    {
		endkb = snapShot();
    }

    // return incremented memory usage in MB
    int used()
    {
        return ( endkb - beginkb )/1024;
    }

    private:
        int  beginkb, endkb; 

		int snapShot()
        {
            char *ptr;
            int sz;
            char fn[256];
            static char line[256];
            pid_t pid = getpid();
            sprintf(fn, "/proc/%d/status", pid );
        
            FILE *fp = fopen(fn, "rb");
            if ( ! fp ) return 0;
        
            while ( NULL != fgets( line, 256, fp) ) {
                sz = strlen(line);
                if ( strstr( line, "VmRSS:") ) {
                    fclose( fp );
                    line[sz-1]='\0';
                    ptr = &line[7];
                    while ( ! isdigit(*ptr) ) ptr++;
                    return atoi(ptr);
                }
            }
            fclose( fp );
            return 0;
        }

};

#endif

