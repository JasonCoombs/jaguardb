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
#include <JagMD5lib.h>
#include <JagFamilyKeyChecker.h>
JagFamilyKeyChecker::JagFamilyKeyChecker( const Jstr &fpathname, int klen, int vlen )
{
	_KLEN = klen;
	_VLEN = vlen; // jdb file k/v len
	_pathName = fpathname;

    if ( klen <= JAG_KEYCHECKER_KLEN ) {
        _UKLEN = klen;
        _useHash = 0;
    } else {
        _UKLEN = JAG_KEYCHECKER_KLEN;
        _useHash = 1;
    }
}

JagFamilyKeyChecker::~JagFamilyKeyChecker()
{
	//delete _lock;
}


// key can be any length
// buf:  [kkk\0\0][vv]  
void JagFamilyKeyChecker::getUniqueKey( const char *buf, char *ukey )
{
	// for short keys (<= JAG_KEYCHECKER_KLEN), use original key
	if ( !_useHash ) {
		memcpy( ukey, buf, _UKLEN );
		ukey[_UKLEN] = '\0';
		return;
	}

	// for longer keys
	char *md5 = MDStringLen( buf, _KLEN );
	unsigned int hash[4];
	unsigned int seed = 42;
	MurmurHash3_x64_128( buf, _KLEN, seed, hash);
	uint64_t res = ((uint64_t*)hash)[0]; 
	memcpy( ukey, md5, JAG_KEYCHECKER_KLEN-6 ); // 16-bytes md5 and then 6-bytes murmur hash
	sprintf( ukey+JAG_KEYCHECKER_KLEN-6, "%0x", res%16777216 ); // 16*16*16*16*16*16 = 16777216
	ukey[JAG_KEYCHECKER_KLEN] = '\0'; // JAG_KEYCHECKER_KLEN==22
	free ( md5 );
}
