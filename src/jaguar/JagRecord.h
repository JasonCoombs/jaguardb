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
#ifndef _jag_record_h_
#define _jag_record_h_
#include <stdio.h>

#define FREC_STR_MAX_LEN 128
#define FREC_HDR_END '^'
#define FREC_VAL_SEP '#'
#define FREC_COMMA ','
#define FREC_MAX_NAMES 256
#define FREC_MAX_NAME_LEN 64

#define JAG_GROUPBY_VALUE_MAX 32
#define JAG_GETALL_VALUE_MAX 1024

class JagRecord
{
   public:
   		JagRecord( ); 
   		JagRecord( const char *srcrec ); 
   		~JagRecord();
		void destroy();

		// load in new source, frees old if not empty
		void  		setSource( const char *srcrec );
        const char *getSource() { return _record; }
		size_t  	getLength();
		void  		readSource( const char *srcrec );
		void		referSource( const char *srcrec );

		void 	    fromJSON( const char *jsonstr );
		void 	    toJSON();

		// add a new pair of name, value into the reccord
        void  addNameValue ( const char *name, const char *value );
        bool  addNameValueArray ( const char *name[], const char *value[], int len );

        bool  remove ( const char *name );

        // 0 or malloced memory
		// Note: you must free the pointer after use
        // char *getValueFromFrec( const char *record, const char *name );
        char *getValue( const char *name );
        
        bool setValue ( const char *name, const char *value );
        
        // int  nameExists(const char *record, const char *name );
		// true: yes, false: no
        int  nameExists( const char *name );

        int  getAllNameValues( char *names[],  char *values[], int *len );
        
		int  compress( const char *type );  // ZLIB
		int  uncompress( );
		void print();


	protected:
		char *_record;
		int   _readOnly;

        void makeNewRecLength( const char *name, int namelen,  const char *value, int vallen );
        int  getNameStartLen( const char *name, int namelen, int *colstart, int *collen );
        int  getSize( int *hdrsize, int *totalsize );

        // 0 or malloced memory
        char *getValueFromPosition( int start, int len, int hdrsize, int recsize );

        // 0 or malloced memory
		// Note: you must free the pointer after use
        char *getValueLength( const char *name, int namelen );

        void  addNameValueLength ( const char *name, int namelen, const char *value, int vallen );

        int  nameLengthExists( const char *name, int namelen );
};


#endif
