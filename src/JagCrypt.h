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
#ifndef _abax_crypt_h_
#define _abax_crypt_h_
#include <abax.h>
#include <tomcrypt.h>
AbaxDataString 		JagDecryptStr( const AbaxDataString &privkey,  const AbaxDataString &str );
AbaxDataString 		JagEncryptStr(const AbaxDataString &pubkey, const AbaxDataString &str );
AbaxDataString 		JagEncryptZFC(ecc_key *pecckey, const AbaxDataString &str );
ecc_key *			JagMakeEccKey( ecc_key *pecckey, AbaxDataString &pubkey, AbaxDataString &privkey );
#endif

