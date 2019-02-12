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
#include <sys/time.h>
#include <JagRange.h>
#include <JagGeom.h>
#include <JagUtil.h>
#include <JagTime.h>

bool 
JagRange::doRangeWithin( const JagParseAttribute &jpa, const Jstr &mk1, const Jstr &colType1, 
						 int srid1, const JagStrSplit &sp1, 
						 const Jstr &mk2, const Jstr &colType2, int srid2, const JagStrSplit &sp2, 
						 bool strict )
{
	/***
	prt(("s6683 doRangeWithin colType2=[%s]\n", colType2.c_str() ));
	//sp1.print();
	//sp2.print();
	prt(("s2220 sp1[-1]=[%s]\n", sp1[-1].c_str() ));
	prt(("s2220 sp2[-1]=[%s]\n", sp2[-1].c_str() ));
	prt(("s2039 srid1=%d srid2=%d\n", srid1, srid2 ));
	***/

	if (  colType2 != JAG_C_COL_TYPE_RANGE ) {
		return false;
	} 

	JagStrSplit ssp( sp2[0], '|' );
	Jstr begin2 = ssp[0]; begin2.replace('_', ' ' );
	Jstr end2 = ssp[1]; end2.replace('_', ' ' );

	if ( colType1 == JAG_C_COL_TYPE_RANGE ) {
		Jstr colobj = sp1[-1];
		JagStrSplit sp( colobj, '=' );
		if ( sp.length() < 5 ) return false;
		Jstr subtype = sp[4];
		Jstr begin1 = sp1[0];
		Jstr end1 = sp1[1];
		return rangeWithinRange( jpa, subtype, begin1, end1, begin2, end2, strict );
	} else {
		Jstr data = sp1[0];
		return pointWithinRange( jpa, colType1, data, begin2, end2, strict );
	}
}

bool JagRange::rangeWithinRange( const JagParseAttribute &jpa, const Jstr &subtype, 
								 const Jstr &begin1, const Jstr &end1,
								 Jstr &begin2, Jstr &end2, bool strict )
{
	//prt(("s2238 rangeWithinRange begin1=[%s] end1=[%s]\n", begin1.c_str(), end1.c_str() ));

	if ( subtype == JAG_C_COL_TYPE_DATE ) {
		if ( ! strchr( begin2.c_str(), '-' ) ) return false;
		if ( ! strchr( end2.c_str(), '-' ) ) return false;
		begin2.remove('-');
		end2.remove('-');
		if ( strict ) {
			if ( strcmp( begin2.c_str(), begin1.c_str() ) < 0 && strcmp( begin2.c_str(), end2.c_str() ) < 0 ) {
				return true;
			}
		} else {
			if ( strcmp( begin2.c_str(), begin1.c_str() ) <= 0 && strcmp( begin2.c_str(), end2.c_str() ) <= 0  ) {
				return true;
			}
		}
	} else if ( subtype == JAG_C_COL_TYPE_TIME ) {
		//prt(("s2234 begin2=[%s] end2=[%s]\n", begin2.c_str(), end2.c_str() ));
		if ( ! strchr( begin2.c_str(), ':' ) ) return false;
		if ( ! strchr( end2.c_str(), ':' ) ) return false;
		Jstr buf1, buf2;
		JagTime::convertTimeToStr( begin1, buf1, 0 );
		JagTime::convertTimeToStr( end1, buf2, 0 );

		//prt(("s2238 buf1=[%s] buf2=[%s]\n", buf1.c_str(), buf2.c_str() ));
		//prt(("s2238 begin2=[%s] end2=[%s]\n", begin2.c_str(), end2.c_str() ));
		if ( strict ) {
			if ( strcmp( begin2.c_str(), buf1.c_str() ) < 0 && strcmp( buf2.c_str(), end2.c_str() ) < 0 ) {
				return true;
			}
		} else {
			if ( strcmp( begin2.c_str(), buf1.c_str() ) <= 0 && strcmp( buf2.c_str(), end2.c_str() ) <= 0 ) {
				return true;
			}
		}
	} else if ( subtype == JAG_C_COL_TYPE_DATETIME ) {
		if ( ! strchr( begin2.c_str(), ':' ) ) return false;
		if ( ! strchr( end2.c_str(), ':' ) ) return false;
		time_t begin1t, end1t;
		begin1t = jagatoll( begin1.c_str() )/1000000;  // microseconds to seconds
		end1t = jagatoll( end1.c_str() )/1000000;  // microseconds to seconds

		time_t begin2t, end2t;
		begin2t = JagTime::getDateTimeFromStr( jpa, begin2.c_str(), false )/1000000;
		end2t = JagTime::getDateTimeFromStr( jpa, end2.c_str(), false )/1000000;

		if ( strict ) {
			if ( begin2t < begin1t && end1t < end2t ) { return true; }
		} else {
			if ( begin2t <= begin1t && end1t <= end2t ) { return true; }
		}
	} else {
		double begin1n = jagatof( begin1.c_str() );
		double end1n = jagatof( end1.c_str() );
		double begin2n = jagatof( begin2.c_str() );
		double end2n = jagatof( end2.c_str() );
		if ( strict ) {
			if ( begin2n < begin1n && end1n < end2n ) {
				return true;
			}
		} else {
			if ( ( (begin2n < begin1n) || jagEQ( begin2n, begin1n ) ) 
			     && ( (end1n < end2n) || jagEQ( end2n, end1n ) ) ) {
				return true;
			}
		}
	}
	
	return false;
}

bool JagRange::pointWithinRange( const JagParseAttribute &jpa, const Jstr &subtype, const Jstr &data, 
								 Jstr &begin2, Jstr &end2, bool strict )
{
	//prt(("s8723 pointWithinRange subtype=[%s]\n", subtype.c_str() ));
	if ( subtype == JAG_C_COL_TYPE_DATE ) {
		if ( ! strchr( begin2.c_str(), '-' ) ) return false;
		if ( ! strchr( end2.c_str(), '-' ) ) return false;
		begin2.remove('-');
		end2.remove('-');
		if ( strict ) {
			if ( strcmp( begin2.c_str(), data.c_str() ) < 0 && strcmp( data.c_str(), end2.c_str() ) < 0 ) {
				return true;
			}
		} else {
			if ( strcmp( begin2.c_str(), data.c_str() ) <= 0 && strcmp( data.c_str(), end2.c_str() ) <= 0  ) {
				return true;
			}
		}
	} else if ( subtype == JAG_C_COL_TYPE_TIME ) {
		//prt(("s6633 begin2.c_str=[%s] end2.c_str=[%s]\n", begin2.c_str(), end2.c_str() ));
		if ( ! strchr( begin2.c_str(), ':' ) ) return false;
		if ( ! strchr( end2.c_str(), ':' ) ) return false;
		Jstr buf;
		JagTime::convertTimeToStr( data, buf, 0 );
		//prt(("s2837 data=[%s] JAG_C_COL_TYPE_TIME buf=[%s] strict=%d\n", data.c_str(), buf.c_str(), strict ));
		//prt(("s2208 strcmp( begin2.c_str(), buf.c_str() )=%d\n", strcmp( begin2.c_str(), buf.c_str() ) ));
		//prt(("s2110 strcmp( buf.c_str(), end2.c_str() ) =%d\n", strcmp( buf.c_str(), end2.c_str() )  ));
		if ( strict ) {
			if ( strcmp( begin2.c_str(), buf.c_str() ) < 0 && strcmp( buf.c_str(), end2.c_str() ) < 0 ) {
				return true;
			}
		} else {
			if ( strcmp( begin2.c_str(), buf.c_str() ) <= 0 && strcmp( buf.c_str(), end2.c_str() ) <= 0 ) {
				return true;
			}
		}
	} else if ( subtype == JAG_C_COL_TYPE_DATETIME ) {
		if ( ! strchr( begin2.c_str(), ':' ) ) return false;
		if ( ! strchr( end2.c_str(), ':' ) ) return false;
		time_t datat;
		datat = jagatoll( data.c_str() )/1000000;

		time_t begin2t, end2t;
		begin2t = JagTime::getDateTimeFromStr( jpa, begin2.c_str(), false )/1000000;
		end2t = JagTime::getDateTimeFromStr( jpa, end2.c_str(), false )/1000000;
		if ( strict ) {
			if ( begin2t < datat && datat < end2t ) { return true; }
		} else {
			if ( begin2t <= datat && datat <= end2t ) { return true; }
		}
	} else {
		double datan = jagatof( data.c_str() );
		double begin2n = jagatof( begin2.c_str() );
		double end2n = jagatof( end2.c_str() );
		if ( strict ) {
			if ( begin2n < datan && datan < end2n ) {
				return true;
			}
		} else {
			if ( ( (begin2n < datan ) || jagEQ( begin2n, datan ) ) 
			     && ( (datan < end2n) || jagEQ( end2n, datan )  ) ) {
				return true;
			}
		}
	}
	
	//prt(("s0201 return false\n" ));
	return false;
}


bool 
JagRange::doRangeIntersect( const JagParseAttribute &jpa, const Jstr &mk1, const Jstr &colType1, 
						    int srid1, const JagStrSplit &sp1, 
						    const Jstr &mk2, const Jstr &colType2, int srid2, const JagStrSplit &sp2 )
{
	/***
	prt(("s6683 doRangeWithin colType2=[%s]\n", colType2.c_str() ));
	sp1.print();
	sp2.print();
	prt(("s2220 sp1[-1]=[%s]\n", sp1[-1].c_str() ));
	prt(("s2220 sp2[-1]=[%s]\n", sp2[-1].c_str() ));
	prt(("s2039 srid1=%d srid2=%d\n", srid1, srid2 ));
	**/

	if (  colType2 != JAG_C_COL_TYPE_RANGE ) {
		//prt(("s2233 false\n" ));
		return false;
	} 

	JagStrSplit ssp( sp2[0], '|' );
	Jstr begin2 = ssp[0]; begin2.replace('_', ' ' );
	Jstr end2 = ssp[1]; end2.replace('_', ' ' );

	if ( colType1 == JAG_C_COL_TYPE_RANGE ) {
		Jstr colobj = sp1[-1];
		JagStrSplit sp( colobj, '=' );
		if ( sp.length() < 5 ) return false;
		Jstr subtype = sp[4];
		Jstr begin1 = sp1[0];
		Jstr end1 = sp1[1];
		return rangeIntersectRange( jpa, subtype, begin1, end1, begin2, end2 );
	} else {
		Jstr data = sp1[0];
		//prt(("s9480 data=[%s]\n", data.c_str() ));
		return pointWithinRange( jpa, colType1, data, begin2, end2, false );
	}
}


bool JagRange::rangeIntersectRange( const JagParseAttribute &jpa, const Jstr &subtype, 
									const Jstr &begin1, const Jstr &end1,
								 	Jstr &begin2, Jstr &end2 )
{
	if ( subtype == JAG_C_COL_TYPE_DATE ) {
		if ( ! strchr( begin2.c_str(), '-' ) ) return false;
		if ( ! strchr( end2.c_str(), '-' ) ) return false;
		begin2.remove('-');
		end2.remove('-');
		if ( strcmp( begin2.c_str(), begin1.c_str() ) <= 0 && strcmp( begin1.c_str(), end2.c_str() ) <= 0  ) {
			return true;
		}
		if ( strcmp( begin2.c_str(), begin2.c_str() ) <= 0 && strcmp( begin2.c_str(), end2.c_str() ) <= 0  ) {
			return true;
		}
	} else if ( subtype == JAG_C_COL_TYPE_TIME ) {
		//strftime (buffer, 20, "%Y-%m-%d %H:%M:%S", timeinfo);
		if ( ! strchr( begin2.c_str(), ':' ) ) return false;
		if ( ! strchr( end2.c_str(), ':' ) ) return false;
		//Jstr buf1 = JagTime::rawToServerString( jpa, rawtime, subtype );
		Jstr buf1, buf2;
		JagTime::convertTimeToStr( begin1, buf1, 0 );
		JagTime::convertTimeToStr( end1, buf2, 0 );
		if ( strcmp( begin2.c_str(), buf1.c_str() ) <= 0 && strcmp( buf1.c_str(), end2.c_str() ) <= 0 ) {
			return true;
		}
		if ( strcmp( begin2.c_str(), buf2.c_str() ) <= 0 && strcmp( buf2.c_str(), end2.c_str() ) <= 0 ) {
			return true;
		}
	} else if ( subtype == JAG_C_COL_TYPE_DATETIME ) {
		if ( ! strchr( begin2.c_str(), ':' ) ) return false;
		if ( ! strchr( end2.c_str(), ':' ) ) return false;
		time_t begin1t, end1t;
		begin1t = jagatoll( begin1.c_str() )/1000000;
		end1t = jagatoll( end1.c_str() )/1000000;
		time_t begin2t, end2t;
		begin2t = JagTime::getDateTimeFromStr( jpa, begin2.c_str(), false )/1000000;
		end2t = JagTime::getDateTimeFromStr( jpa, end2.c_str(), false )/1000000;
		if ( begin2t <= begin1t && begin1t <= end2t ) { return true; }
		if ( begin2t <= end1t && end1t <= end2t ) { return true; }
	} else {
		double begin1n = jagatof( begin1.c_str() );
		double end1n = jagatof( end1.c_str() );
		double begin2n = jagatof( begin2.c_str() );
		double end2n = jagatof( end2.c_str() );
		if ( ( (begin2n < begin1n) || jagEQ( begin2n, begin1n ) ) 
		     && ( (begin1n < end2n) || jagEQ( begin1n, end2n ) ) ) {
			return true;
		}

		if ( ( (begin2n < end1n) || jagEQ( begin2n, end1n ) ) 
		     && ( (end1n < end2n) || jagEQ( end1n, end2n ) ) ) {
			return true;
		}
	}
	
	return false;
}


bool 
JagRange::doRangeSame( const JagParseAttribute &jpa, const Jstr &mk1, const Jstr &colType1, 
						 int srid1, const JagStrSplit &sp1, 
						 const Jstr &mk2, const Jstr &colType2, int srid2, const JagStrSplit &sp2 )
{
	/***
	prt(("s6683 doRangeWithin colType2=[%s]\n", colType2.c_str() ));
	sp1.print();
	sp2.print();
	prt(("s2220 sp1[-1]=[%s]\n", sp1[-1].c_str() ));
	prt(("s2220 sp2[-1]=[%s]\n", sp2[-1].c_str() ));
	prt(("s2039 srid1=%d srid2=%d\n", srid1, srid2 ));
	***/

	if (  colType2 != JAG_C_COL_TYPE_RANGE ) {
		return false;
	} 

	JagStrSplit ssp( sp2[0], '|' );
	Jstr begin2 = ssp[0]; begin2.replace('_', ' ' );
	Jstr end2 = ssp[1]; end2.replace('_', ' ' );

	Jstr colobj = sp1[-1];
	JagStrSplit sp( colobj, '=' );
	if ( sp.length() < 5 ) return false;
	Jstr subtype = sp[4];
	Jstr begin1 = sp1[0];
	Jstr end1 = sp1[1];
	return rangeSameRange( jpa, subtype, begin1, end1, begin2, end2 );
}

bool JagRange::rangeSameRange( const JagParseAttribute &jpa, const Jstr &subtype, 
								 const Jstr &begin1, const Jstr &end1,
								 Jstr &begin2, Jstr &end2 )
{
	//prt(("s2238 rangeWithinRange begin1=[%s] end1=[%s]\n", begin1.c_str(), end1.c_str() ));

	if ( subtype == JAG_C_COL_TYPE_DATE ) {
		if ( ! strchr( begin2.c_str(), '-' ) ) return false;
		if ( ! strchr( end2.c_str(), '-' ) ) return false;
		begin2.remove('-');
		end2.remove('-');
		if ( begin2 == begin1 && end2 == end1 ) return true;
	} else if ( subtype == JAG_C_COL_TYPE_TIME ) {
		//prt(("s2234 begin2=[%s] end2=[%s]\n", begin2.c_str(), end2.c_str() ));
		if ( ! strchr( begin2.c_str(), ':' ) ) return false;
		if ( ! strchr( end2.c_str(), ':' ) ) return false;
		Jstr buf1, buf2;
		JagTime::convertTimeToStr( begin1, buf1, 0 );
		JagTime::convertTimeToStr( end1, buf2, 0 );

		if ( buf1 == begin2 && buf2 == end2 ) return true;
	} else if ( subtype == JAG_C_COL_TYPE_DATETIME ) {
		if ( ! strchr( begin2.c_str(), ':' ) ) return false;
		if ( ! strchr( end2.c_str(), ':' ) ) return false;
		time_t begin1t, end1t;
		begin1t = jagatoll( begin1.c_str() )/1000000;  // microseconds to seconds
		end1t = jagatoll( end1.c_str() )/1000000;  // microseconds to seconds

		time_t begin2t, end2t;
		begin2t = JagTime::getDateTimeFromStr( jpa, begin2.c_str(), false )/1000000;
		end2t = JagTime::getDateTimeFromStr( jpa, end2.c_str(), false )/1000000;
		if ( begin2t == begin1t && end1t == end2t ) return true;
	} else {
		double begin1n = jagatof( begin1.c_str() );
		double end1n = jagatof( end1.c_str() );
		double begin2n = jagatof( begin2.c_str() );
		double end2n = jagatof( end2.c_str() );
		if ( begin2n == begin1n && end1n == end2n ) return true;
	}
	
	return false;
}
