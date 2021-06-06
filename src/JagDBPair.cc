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
#include "JagGlobalDef.h"
#include "JagDBPair.h"

int JagDBPair::compareKeys( const JagDBPair &d2 ) const {
	if ( key.addr() == NULL || key.addr()[0] == '\0' ) {
		if ( d2.key.size()<1 || d2.key.addr() == NULL || d2.key.addr()[0] == '\0' ) {
			return 0;
		} else {
			return -1;
		}
	} else {
		if ( d2.key.addr() == NULL || d2.key.addr()[0] == '\0' ) {
			return 1;
		} else {
			if ( key.addr()[0] == '*' && d2.key.addr()[0] == '*' ) {
				return 0;
			} else {
				return ( memcmp(key.addr(), d2.key.addr(), key.size() ) );
			}
		}
	}
}

void JagDBPair::print() const {
	int i;
	printf("K: ");
	for (i=0; i<key.size(); ++i ) {
		printf("[%d]%c ", i, key.c_str()[i] );
	}
	printf("\n");
	printf("V: ");
	for (i=0; i<value.size(); ++i ) {
		if ( value.c_str()[i] ) {
			printf("[%d]%c ", i, value.c_str()[i] );
		} else {
			printf("[%d]@ ", i );
		}
	}
	printf("\n");
	fflush(stdout);
}

void JagDBPair::printkv() const {
	printf("[%s][%s]\n", key.s(), value.s() );
	fflush(stdout);
}

void JagDBPair::toBuffer(char *buffer) const {
	memcpy(buffer, key.c_str(), key.size() );
	memcpy(buffer + key.size(), value.c_str(), value.size() );
}

char *JagDBPair::newBuffer() const {
	char *buffer = jagmalloc( key.size() + value.size() +1 );
	buffer [ key.size() + value.size() ] = '\0';
	memcpy(buffer, key.c_str(), key.size() );
	memcpy(buffer + key.size(), value.c_str(), value.size() );
	return buffer;
}

