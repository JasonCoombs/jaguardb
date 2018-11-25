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
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <abax.h>
#include <JagCrypt.h>
#include <JagStrSplit.h>
#include <JagMD5lib.h>
#include <JagNet.h>
#include <base64.h>
#include <JagMD5lib.h>
#include <JagStrSplit.h>
#include "JagFileMgr.h"
#include "JagUtil.h"


// zfc: zi fu chuan: string
//AbaxDataString abaxJiesuoZFC( const AbaxDataString &privkey, const AbaxDataString &src64 )
AbaxDataString JagDecryptStr( const AbaxDataString &privkey, const AbaxDataString &src64 )
{
	unsigned char 	plainmsg[512];
	ecc_key  		ecckey;
	unsigned long 	outlen;
    AbaxDataString 	pkey = abaxDecodeBase64( privkey );
    AbaxDataString 	src = abaxDecodeBase64( src64 );

	ltc_mp = tfm_desc;
	register_hash(&sha512_desc);

	int rc = ecc_import( (unsigned char*)pkey.c_str(), pkey.size(), &ecckey );
	if ( rc != CRYPT_OK ) {
		// debug
		// printf("e1239 import key error [%s]\n", error_to_string( rc ) );
		return "";
	}

	outlen = 512;
	memset( plainmsg, 0, outlen);
	rc = ecc_decrypt_key( (unsigned char*)src.c_str(), src.size(), plainmsg, &outlen, &ecckey );
	if ( rc != CRYPT_OK ) {
		// debug
		// printf("e1229 dcrypt error [%s]\n", error_to_string( rc ));
		// printf("src=[%s] size=%d\n", src.c_str(), src.size() );
		return "";
	}

	AbaxDataString astr( (const char*)plainmsg, outlen );
	return astr;
}




// ptr: OK
// NULL: bad
//ecc_key *makeEccKey( ecc_key *pecckey, AbaxDataString &pubkey, AbaxDataString &privkey )
ecc_key *JagMakeEccKey( ecc_key *pecckey, AbaxDataString &pubkey, AbaxDataString &privkey )
{
	int wprng;
	int hash;
	int rc;
	prng_state  prng;
	unsigned long outlen;
	unsigned char pub[512];
	unsigned char priv[512];

	register_prng( &sprng_desc );
	register_prng( &yarrow_desc );
	wprng = find_prng( "sprng" );
	ltc_mp = tfm_desc;
	register_hash(&sha512_desc);
	hash = find_hash ( "sha512");

	rc = rng_make_prng( 256, wprng, &prng, NULL);  
	if (rc != CRYPT_OK) {
          return NULL;
    }

	rc = ecc_make_key( &prng, wprng, 48, pecckey );  // 32, 48, 65
	if ( rc != CRYPT_OK ) {
		return NULL;
	}

	outlen = 512;
	rc = ecc_export( pub, &outlen, PK_PUBLIC, pecckey );
	if ( rc != CRYPT_OK ) {
		return NULL;
	}

	AbaxDataString newp( (const char*)pub, outlen );
	pubkey = abaxEncodeBase64 ( newp );

	outlen = 512;
	rc = ecc_export( priv, &outlen, PK_PRIVATE, pecckey );
	if ( rc != CRYPT_OK ) {
		return NULL;
	}

	AbaxDataString newv( (const char*)priv, outlen );
	privkey = abaxEncodeBase64 ( newv );

	return pecckey;
}

AbaxDataString JagEncryptStr( const AbaxDataString &pubkey, const AbaxDataString &src )
{
	// import pbkey to ecckey
	ecc_key  		ecckey;
	unsigned long 	outlen;
    AbaxDataString 	pkey = abaxDecodeBase64( pubkey );

	ltc_mp = tfm_desc;

	int rc = ecc_import( (unsigned char*)pkey.c_str(), pkey.size(), &ecckey );
	if ( rc != CRYPT_OK ) {
		printf("e0134  rc != CRYPT_OK pkey.size()=%d pkey.c_str=[%s]\n", pkey.size(), pkey.c_str()  );
		return "";
	}

	return JagEncryptZFC( &ecckey, src );
}

AbaxDataString JagEncryptZFC( ecc_key *pecckey, const AbaxDataString &src )
{
	int rc;
	int hash;
	int wprng;
	prng_state  prng;
	unsigned char ciphermsg[512];
	unsigned long outlen;

	ltc_mp = tfm_desc;

	register_prng( &sprng_desc );
	register_prng( &yarrow_desc );
	wprng = find_prng( "sprng" );

	register_hash(&sha512_desc);
	hash = find_hash ( "sha512");

	rc = rng_make_prng( 256, wprng, &prng, NULL);  
	if (rc != CRYPT_OK) {
          return "";
    }

    // rc = ecc_encrypt_key( msg, msglen, ciphermsg, &outlen1, &prng,  wprng, hash, &ecckey );
	outlen = 512;
    rc = ecc_encrypt_key( (unsigned char*)src.c_str(), src.size(), ciphermsg, &outlen, &prng,  wprng, hash, pecckey );
	if (rc != CRYPT_OK) {
          return "";
    }

	AbaxDataString enc( (const char*)ciphermsg, outlen );
	return ( abaxEncodeBase64 ( enc ) );
}

