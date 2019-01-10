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
#ifndef _jag_range_h_
#define _jag_range_h_

#include <JagDef.h>
#include <JagVector.h>
#include <JagUtil.h>
#include <JagParseAttribute.h>

class JagRange
{
  public:

   	static bool 
	doRangeWithin( const JagParseAttribute &jpa, const AbaxDataString &mk1, const AbaxDataString &colType1, int srid1, const JagStrSplit &sp1, 
				   const AbaxDataString &mk2, const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, 
				   bool strict=true );
   	static bool 
	doRangeIntersect( const JagParseAttribute &jpa, const AbaxDataString &mk1, const AbaxDataString &colType1, int srid1, const JagStrSplit &sp1, 
				   const AbaxDataString &mk2, const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2 );

	static bool rangeWithinRange( const JagParseAttribute &jpa, const AbaxDataString &subtype, const AbaxDataString &begin1, const AbaxDataString &end1,
	                              AbaxDataString &begin2, AbaxDataString &end2, bool strict );

	static bool pointWithinRange( const JagParseAttribute &jpa, const AbaxDataString &subtype, const AbaxDataString &data,
	                              AbaxDataString &begin2, AbaxDataString &end2, bool strict );

	static bool rangeIntersectRange( const JagParseAttribute &jpa, const AbaxDataString &subtype, const AbaxDataString &begin1, const AbaxDataString &end1,
	                              AbaxDataString &begin2, AbaxDataString &end2 );
   	static bool doRangeSame( const JagParseAttribute &jpa, const AbaxDataString &mk1, const AbaxDataString &colType1, 
							 int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
							 int srid2, const JagStrSplit &sp2 );
	static bool rangeSameRange( const JagParseAttribute &jpa, const AbaxDataString &subtype, const AbaxDataString &begin1, const AbaxDataString &end1,
	                              AbaxDataString &begin2, AbaxDataString &end2 );

};


#endif
