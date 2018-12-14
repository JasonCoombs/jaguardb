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
#include <math.h>
#include <JagGeom.h>
#include <JagArray.h>
#include <GeographicLib/Gnomonic.hpp>
#include <GeographicLib/Geodesic.hpp>
#include <GeographicLib/Geocentric.hpp>
#include <GeographicLib/PolygonArea.hpp>
#include <GeographicLib/Constants.hpp>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <JaguarCPPClient.h>
#include <JagParser.h>

using namespace GeographicLib;

const double JagGeo::ZERO = 0.0000000001;
const double JagGeo::NUM_SAMPLE = 10;

JagPoint2D::JagPoint2D()
{
}

JagPoint2D::JagPoint2D( const char *sx, const char *sy)
{
	x = jagatof(sx); y = jagatof( sy );
}

JagPoint2D::JagPoint2D( double inx, double iny )
{
	x = inx; y = iny;
}

JagPoint3D::JagPoint3D()
{
}

JagPoint3D::JagPoint3D( const char *sx, const char *sy, const char *sz)
{
	x = jagatof(sx); y = jagatof( sy ); z = jagatof( sz );
}

JagPoint3D::JagPoint3D( double inx, double iny, double inz )
{
	x = inx; y = iny; z = inz;
}

bool JagSortPoint2D::operator<( const JagSortPoint2D &o) const
{
	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( x1 < o.x1 ) return true;
		} else {
			if ( x1 < o.x2 ) return true;
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( x2 < o.x1 ) return true;
		} else {
			if ( x2 < o.x2 ) return true;
		}
	}

	return false;
}
bool JagSortPoint2D::operator<=( const JagSortPoint2D &o) const
{
	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( x1 < o.x1 ) return true;
		} else {
			if ( x1 < o.x2 ) return true;
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( x2 < o.x1 ) return true;
		} else {
			if ( x2 < o.x2 ) return true;
		}
	}

	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( JagGeo::jagEQ(x1, o.x1) ) return true;
		} else {
			if ( JagGeo::jagEQ(x1, o.x2) ) return true;
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( JagGeo::jagEQ(x2, o.x1) ) return true;
		} else {
			if ( JagGeo::jagEQ(x2, o.x2) ) return true;
		}
	}
	return false;
}

bool JagSortPoint2D::operator>( const JagSortPoint2D &o) const
{
	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( x1 > o.x1 ) return true;
		} else {
			if ( x1 > o.x2 ) return true;
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( x2 > o.x1 ) return true;
		} else {
			if ( x2 > o.x2 ) return true;
		}
	}
	return false;
}


bool JagSortPoint2D::operator>=( const JagSortPoint2D &o) const
{
	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( x1 > o.x1 ) return true;
		} else {
			if ( x1 > o.x2 ) return true;
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( x2 > o.x1 ) return true;
		} else {
			if ( x2 > o.x2 ) return true;
		}
	}

	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( JagGeo::jagEQ(x1, o.x1) ) return true;
		} else {
			if ( JagGeo::jagEQ(x1, o.x2) ) return true;
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( JagGeo::jagEQ(x2, o.x1) ) return true;
		} else {
			if ( JagGeo::jagEQ(x2, o.x2) ) return true;
		}
	}

	return false;
}


bool JagSortPoint3D::operator<( const JagSortPoint3D &o) const
{
	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( x1 < o.x1 ) return true;
			if ( JagGeo::jagEQ( x1, o.x1) ) {
				if ( y1 < o.y1 ) return true;
			}
		} else {
			if ( x1 < o.x2 ) return true;
			if ( JagGeo::jagEQ( x1, o.x2) ) {
				if ( y1 < o.y2 ) return true;
			}
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( x2 < o.x1 ) return true;
			if ( JagGeo::jagEQ( x2, o.x1) ) {
				if ( y2 < o.y1 ) return true;
			}
		} else {
			if ( x2 < o.x2 ) return true;
			if ( JagGeo::jagEQ( x2, o.x2) ) {
				if ( y2 < o.y2 ) return true;
			}
		}
	}

	return false;
}

bool JagSortPoint3D::operator<=( const JagSortPoint3D &o) const
{

	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( x1 < o.x1 ) return true;
			if ( JagGeo::jagEQ( x1, o.x1) ) {
				if ( y1 < o.y1 ) return true;
			}
		} else {
			if ( x1 < o.x2 ) return true;
			if ( JagGeo::jagEQ( x1, o.x2) ) {
				if ( y1 < o.y2 ) return true;
			}
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( x2 < o.x1 ) return true;
			if ( JagGeo::jagEQ( x2, o.x1) ) {
				if ( y2 < o.y1 ) return true;
			}
		} else {
			if ( x2 < o.x2 ) return true;
			if ( JagGeo::jagEQ( x2, o.x2) ) {
				if ( y2 < o.y2 ) return true;
			}
		}
	}


	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( JagGeo::jagEQ(x1, o.x1) && JagGeo::jagEQ(y1, o.y1) ) return true;
		} else {
			if ( JagGeo::jagEQ(x1, o.x2) && JagGeo::jagEQ(y1, o.y2) ) return true;
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( JagGeo::jagEQ(x2, o.x1) && JagGeo::jagEQ(y2, o.y1) ) return true;
		} else {
			if ( JagGeo::jagEQ(x2, o.x2) && JagGeo::jagEQ(y2, o.y2) ) return true;
		}
	}
	return false;
}

bool JagSortPoint3D::operator>( const JagSortPoint3D &o) const
{
	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( x1 > o.x1 ) return 1;
			if ( JagGeo::jagEQ( x1, o.x1) ) {
				if ( y1 > o.y1 ) return -1;
			}
		} else {
			if ( x1 > o.x2 ) return 1;
			if ( JagGeo::jagEQ( x1, o.x2) ) {
				if ( y1 > o.y2 ) return 1;
			}
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( x2 > o.x1 ) return 1;
			if ( JagGeo::jagEQ( x2, o.x1) ) {
				if ( y2 > o.y1 ) return 1;
			}
		} else {
			if ( x2 > o.x2 ) return 1;
			if ( JagGeo::jagEQ( x2, o.x2) ) {
				if ( y2 > o.y2 ) return 1;
			}
		}
	}

	return false;
}


bool JagSortPoint3D::operator>=( const JagSortPoint3D &o) const
{
	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( x1 > o.x1 ) return true;
			if ( JagGeo::jagEQ( x1, o.x1) ) {
				if ( y1 > o.y1 ) return true;
			}
		} else {
			if ( x1 > o.x2 ) return true;
			if ( JagGeo::jagEQ( x1, o.x2) ) {
				if ( y1 > o.y2 ) return true;
			}
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( x2 > o.x1 ) return true;
			if ( JagGeo::jagEQ( x2, o.x1) ) {
				if ( y2 > o.y1 ) return true;
			}
		} else {
			if ( x2 > o.x2 ) return true;
			if ( JagGeo::jagEQ( x2, o.x2) ) {
				if ( y2 > o.y2 ) return true;
			}
		}
	}

	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( JagGeo::jagEQ(x1, o.x1) && JagGeo::jagEQ(y1, o.y1) ) return true;
		} else {
			if ( JagGeo::jagEQ(x1, o.x2) && JagGeo::jagEQ(y1, o.y2) ) return true;
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( JagGeo::jagEQ(x2, o.x1) && JagGeo::jagEQ(y2, o.y1) ) return true;
		} else {
			if ( JagGeo::jagEQ(x2, o.x2) && JagGeo::jagEQ(y2, o.y2) ) return true;
		}
	}

	return false;
}


JagPoint::JagPoint() { init(); }
JagPoint::JagPoint( const char *inx, const char *iny )
{
	init();
	strcpy( x, inx );
	strcpy( y, iny );
}

JagPoint::JagPoint( const char *inx, const char *iny, const char *inz )
{
	init();
	strcpy( x, inx );
	strcpy( y, iny );
	strcpy( z, inz );
}

JagPoint& JagPoint::operator=( const JagPoint& p2 ) 
{
	if ( this == &p2 ) { return *this; }
	copyData( p2 );
	return *this; 
}


JagPoint::JagPoint( const JagPoint& p2 ) 
{
	copyData( p2 );
}
void JagPoint::init() 
{
	memset( x, 0, JAG_POINT_LEN); memset(y, 0, JAG_POINT_LEN ); memset( z, 0, JAG_POINT_LEN); 
	memset( a, 0, JAG_POINT_LEN); memset( b, 0, JAG_POINT_LEN); memset( c, 0, JAG_POINT_LEN);
	memset( nx, 0, JAG_POINT_LEN); memset( ny, 0, JAG_POINT_LEN); 
}

void JagPoint::copyData( const JagPoint& p2 )
{
	memcpy( x, p2.x, JAG_POINT_LEN); 
	memcpy( y, p2.y, JAG_POINT_LEN ); 
	memcpy( z, p2.z, JAG_POINT_LEN); 
	memcpy( a, p2.a, JAG_POINT_LEN );
	memcpy( b, p2.b, JAG_POINT_LEN );
	memcpy( c, p2.c, JAG_POINT_LEN );
	memcpy( nx, p2.nx, JAG_POINT_LEN );
	memcpy( ny, p2.ny, JAG_POINT_LEN );
}

void JagPoint::print()  const
{
	prt(("x=[%s] y=[%s] z=[%s] a=[%s] b=[%s] c=[%s] nx=[%s] ny=[%s]\n", x,y,z,a,b,c,nx,ny ));

}

JagLineString& JagLineString::operator=( const JagLineString3D& L2 )
{
	init();
	for ( int i=0; i < L2.size(); ++i ) {
		add( L2.point[i] );
	}
	return *this;
}


void JagLineString::add( const JagPoint2D &p )
{
	JagPoint pp( doubleToStr(p.x).c_str(),  doubleToStr(p.y).c_str() );
	point.append(pp);
}

void JagLineString::add( const JagPoint3D &p )
{
	JagPoint pp( doubleToStr(p.x).c_str(),  doubleToStr(p.y).c_str(), doubleToStr(p.z).c_str() );
	point.append(pp);
}

void JagLineString::add( double x, double y )
{
	JagPoint pp( doubleToStr(x).c_str(),  doubleToStr(y).c_str() );
	point.append(pp);
}

void JagLineString::center2D( double &cx, double &cy, bool dropLast ) const
{
	cx = cy = 0.0;

	int len = point.size();
	if ( dropLast) --len;
	if ( len < 1 ) return;
	for ( int i=0; i < len; ++i ) {
		cx += jagatof(point[i].x);
		cy += jagatof(point[i].y);
	}
	cx = cx/len;
	cy = cy/len;
}
void JagLineString::center3D( double &cx, double &cy, double &cz, bool dropLast ) const
{
	cx = cy = cz = 0.0;
	int len = point.size();
	if ( dropLast) --len;
	if ( len < 1 ) return;
	for ( int i=0; i < len; ++i ) {
		cx += jagatof(point[i].x);
		cy += jagatof(point[i].y);
		cz += jagatof(point[i].z);
	}
	cx = cx/len;
	cy = cy/len;
	cz = cz/len;
}


void JagLineString3D::add( const JagPoint2D &p )
{
	JagPoint3D pp( doubleToStr(p.x).c_str(),  doubleToStr(p.y).c_str(), "0.0" );
	point.append(pp);
}

void JagLineString3D::add( const JagPoint3D &p )
{
	JagPoint3D pp( doubleToStr(p.x).c_str(),  doubleToStr(p.y).c_str(), doubleToStr(p.z).c_str() );
	point.append(pp);
}

void JagLineString3D::add( double x, double y, double z )
{
	JagPoint3D pp;
	pp.x = x; pp.y = y; pp.z = z;
	point.append(pp);
}

void JagLineString3D::center3D( double &cx, double &cy, double &cz, bool dropLast ) const
{
	cx = cy = cz = 0.0;
	int len = point.size();
	if ( dropLast ) --len;
	if ( len < 1 ) return;
	for ( int i=0; i < len; ++i ) {
		cx += point[i].x;
		cy += point[i].y;
		cz += point[i].z;
	}
	cx = cx/len;
	cy = cy/len;
	cz = cz/len; 
}

void JagLineString3D::center2D( double &cx, double &cy, bool dropLast ) const
{
	cx = cy = 0.0;
	int len = point.size();
    if ( dropLast ) --len;
   	if ( len < 1 ) return;
	for ( int i=0; i < len; ++i ) {
		cx += point[i].x;
		cy += point[i].y;
	}
	cx = cx/len;
	cy = cy/len;
}


///////////////////////////// Within methods ////////////////////////////////////////
bool JagGeo::pointInTriangle( double px, double py, double x1, double y1,
                              double x2, double y2, double x3, double y3,
                              bool strict, bool boundcheck )
{
	JagPoint2D point(px,py);
	JagPoint2D p1(x1,y1);
	JagPoint2D p2(x2,y2);
	JagPoint2D p3(x3,y3);
	return pointWithinTriangle( point, p1, p2, p3, strict, boundcheck );
}

// strict true: point strictly inside (no boundary) in triangle.
//        false: point can be on boundary
// boundcheck:  do bounding box check or not
bool JagGeo::pointWithinTriangle( const JagPoint2D &point,
                              const JagPoint2D &point1, const JagPoint2D &point2,
                              const JagPoint2D &point3, bool strict, bool boundcheck )
{
	double f;
	if ( boundcheck ) {
		f = ( point1.x < point2.x ) ?  point1.x : point2.x;
		f = (  f < point3.x ) ?  f : point3.x;
		if ( point.x < ( f - 0.01 ) ) { return false; }

		f = ( point1.y < point2.y ) ?  point1.y : point2.y;
		f = ( f < point3.y ) ?  f : point3.y;
		if ( point.y < ( f - 0.01 ) ) { return false; }


		f = ( point1.x > point2.x ) ?  point1.x : point2.x;
		f = (  f > point3.x ) ?  f : point3.x;
		if ( point.x > ( f + 0.01 ) ) { return false; }

		f = ( point1.y > point2.y ) ?  point1.y : point2.y;
		f = ( f > point3.y ) ?  f : point3.y;
		if ( point.y > ( f + 0.01 ) ) { return false; }
	}

	// simple check
	bool b1, b2, b3;
	b1=b2=b3=false;
	f = doSign( point, point1, point2 ); 
	if ( f <= 0.0 ) b1 = true;

	f = doSign( point, point2, point3 ); 
	if ( f <= 0.0 ) b2 = true;

	f = doSign( point, point3, point1 ); 
	if ( f <= 0.0 ) b3 = true;

	if ( b1 == b2 && b2 == b3 ) {
		return true;
	}

	if ( strict ) {
		return false;
	}

	// b1==b2==b3
 	if (distSquarePointToSeg( point, point1, point2 ) <= ZERO) {
		return true;
	}

 	if (distSquarePointToSeg( point, point2, point3 ) <= ZERO) {
		return true;
	}

 	if (distSquarePointToSeg( point, point3, point1 ) <= ZERO) {
		return true;
	}

	return false;
}

bool JagGeo::doPointWithin( const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	if ( colType2 == JAG_C_COL_TYPE_POINT ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		return pointWithinPoint( px0, py0, x0, y0, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_LINE ) {
		double x1 = jagatof( sp2[0].c_str() ); 
		double y1 = jagatof( sp2[1].c_str() ); 
		double x2 = jagatof( sp2[2].c_str() ); 
		double y2 = jagatof( sp2[3].c_str() ); 
		return pointWithinLine( px0, py0, x1, y1, x2, y2, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING || colType2 == JAG_C_COL_TYPE_MULTIPOINT ) {
		return pointWithinLineString( px0, py0, mk2, sp2, strict );
	} else if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
    	JagPoint2D point( sp1[0].c_str(), sp1[1].c_str() );
    	JagPoint2D p1( sp2[0].c_str(), sp2[1].c_str() );
    	JagPoint2D p2( sp2[2].c_str(), sp2[3].c_str() );
    	JagPoint2D p3( sp2[4].c_str(), sp2[5].c_str() );
		return pointWithinTriangle( point, p1, p2, p3, strict, true );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3);
		double lon = meterToLon( srid2, r, x0, y0);
		double lat = meterToLat( srid2, r, x0, y0);
		return pointWithinRectangle( px0, py0, x0, y0, lon, lat, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double w = jagatof( sp2[2].c_str() ); 
		double h = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double lon = meterToLon( srid2, w, x0, y0);
		double lat = meterToLat( srid2, h, x0, y0);
		return pointWithinRectangle( px0, py0, x0, y0, lon, lat, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() );
		double lon = meterToLon( srid2, r, x, y);
		double lat = meterToLat( srid2, r, x, y);
		// return pointWithinCircle( px0, py0, x, y, r, strict );
		return pointWithinEllipse( px0, py0, x, y, lon, lat, 0.0, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double w = jagatof( sp2[2].c_str() );
		double h = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		double lon = meterToLon( srid2, w, x0, y0);
		double lat = meterToLat( srid2, h, x0, y0);
		return pointWithinEllipse( px0, py0, x0, y0, lon, lat, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return pointWithinPolygon( px0, py0, mk2, sp2, strict );
	}
	return false;
}

bool JagGeo::doPoint3DWithin( const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	//prt(("s4409 doPoint3DWithin colType2=[%s]\n", colType2.c_str() ));

	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	if ( colType2 == JAG_C_COL_TYPE_POINT3D ) {
		double x1 = jagatof( sp2[0].c_str() ); 
		double y1 = jagatof( sp2[1].c_str() ); 
		double z1 = jagatof( sp2[2].c_str() ); 
		return point3DWithinPoint3D( px0, py0, pz0, x1, y1, z1, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_LINE3D ) {
		double x1 = jagatof( sp2[0].c_str() ); 
		double y1 = jagatof( sp2[1].c_str() ); 
		double z1 = jagatof( sp2[2].c_str() ); 
		double x2 = jagatof( sp2[3].c_str() ); 
		double y2 = jagatof( sp2[4].c_str() ); 
		double z2 = jagatof( sp2[5].c_str() ); 
		return point3DWithinLine3D( px0, py0, pz0, x1, y1, z1, x2, y2, z2, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING3D || colType2 == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		return point3DWithinLineString3D( px0, py0, pz0, mk2, sp2, strict );
	} else if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return point3DWithinBox( px0, py0, pz0, x0, y0, z0, r,r,r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double a = jagatof( sp2[3].c_str() ); 
		double b = jagatof( sp2[4].c_str() ); 
		double c = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return point3DWithinBox( px0, py0, pz0, x0, y0, z0, a,b,c, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return point3DWithinSphere( px0,py0,pz0, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double a = jagatof( sp2[3].c_str() );
		double b = jagatof( sp2[4].c_str() );
		double c = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return point3DWithinEllipsoid( px0, py0, pz0, x0, y0, z0, a,b,c, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return point3DWithinCone( px0,py0,pz0, x, y, z, r, h, nx, ny, strict );
	}
	return false;
}

double JagGeo::doCircleArea( int srid1, const JagStrSplit &sp1 )
{
	//double px0 = jagatof( sp1[0].c_str() ); 
	//double py0 = jagatof( sp1[1].c_str() ); 
	double r = jagatof( sp1[2].c_str() ); 
	return r * r * JAG_PI;
}
bool JagGeo::doCircleWithin( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pr0 = jagatof( sp1[2].c_str() ); 
	pr0 = meterToLon( srid2, pr0, px0, py0);

	if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() );
		r = meterToLon( srid2, r, x, y);
		return circleWithinCircle(px0,py0,pr0, x,y,r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() );
		r = meterToLon( srid2, r, x, y);
		double nx = safeget( sp2, 3);
		return circleWithinSquare(px0,py0,pr0, x,y,r, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double w = jagatof( sp2[2].c_str() );
		double h = jagatof( sp2[3].c_str() );
		w = meterToLon( srid2, w, x0, y0);
		h = meterToLat( srid2, h, x0, y0);
		double nx = safeget( sp2, 4);
		return circleWithinRectangle( px0, py0, pr0, x0, y0, w, h, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		double x1 = jagatof( sp2[0].c_str() ); 
		double y1 = jagatof( sp2[1].c_str() ); 
		double x2 = jagatof( sp2[2].c_str() ); 
		double y2 = jagatof( sp2[3].c_str() ); 
		double x3 = jagatof( sp2[4].c_str() ); 
		double y3 = jagatof( sp2[5].c_str() ); 
		return circleWithinTriangle(px0, py0, pr0, x1, y1, x2, y2, x3, y3, strict, true );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		a = meterToLon( srid2, a, x0, y0);
		b = meterToLat( srid2, b, x0, y0);
		double nx = safeget( sp2, 4);
		return circleWithinEllipse(px0, py0, pr0, x0, y0, a, b, nx, strict, true );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return circleWithinPolygon(px0, py0, pr0, mk2, sp2, strict );
	}
	return false;
}

// circle surface with x y z and orientation
bool JagGeo::doCircle3DWithin( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double pr0 = jagatof( sp1[3].c_str() ); 

	double nx0 = 0.0;
	double ny0 = 0.0;
	if ( sp1.length() >= 5 ) { nx0 = jagatof( sp1[4].c_str() ); }
	if ( sp1.length() >= 6 ) { ny0 = jagatof( sp1[5].c_str() ); }

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return circle3DWithinCube( px0, py0, pz0, pr0, nx0, ny0, x0, y0, z0, r, nx, ny, strict );
	} else if (  colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double a = jagatof( sp2[3].c_str() ); 
		double b = jagatof( sp2[4].c_str() ); 
		double c = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return circle3DWithinBox( px0, py0, pz0, pr0, nx0, ny0, x0, y0, z0, a,b,c, nx, ny, strict );
	} else if (  colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return circle3DWithinSphere( px0, py0, pz0, pr0, nx0, ny0, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return circle3DWithinEllipsoid( px0, py0, pz0, pr0, nx0, ny0, x0, y0, z0, w,d,h,  nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5);
		double ny = safeget( sp2, 6);
		return circle3DWithinCone( px0,py0,pz0,pr0,nx0,ny0, x, y, z, r, h, nx, ny, strict );
	}

	return false;
}

bool JagGeo::doSphereWithin( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double pr0 = jagatof( sp1[2].c_str() ); 
	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4);
		double ny = safeget( sp2, 5);
		return sphereWithinCube( px0, py0, pz0, pr0, x0, y0, z0, r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 

		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return sphereWithinBox( px0, py0, pz0, pr0, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return sphereWithinSphere( px0, py0, pz0, pr0, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return sphereWithinEllipsoid( px0, py0, pz0, pr0, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5);
		double ny = safeget( sp2, 6);
		return sphereWithinCone( px0, py0, pz0, pr0,    x0, y0, z0, r,h, nx,ny, strict );
	}
	return false;
}

// 2D
bool JagGeo::doSquareWithin( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	//prt(("s3033 doSquareWithin colType2=[%s] \n", colType2.c_str() ));
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pr0 = jagatof( sp1[2].c_str() ); 
	pr0 = meterToLon( srid2, pr0, px0, py0 );
	double nx0 = safeget( sp1, 3 );

	// like point within
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return rectangleWithinTriangle( px0, py0, pr0,pr0, nx0, x1, y1, x2, y2, x3, y3, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		a = meterToLon( srid2, a, x0, y0 );
		double nx = safeget(sp2, 3 );
		return rectangleWithinSquare( px0, py0, pr0,pr0, nx0, x0, y0, a, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		a = meterToLon( srid2, a, x0, y0 );
		b = meterToLat( srid2, b, x0, y0 );
		double nx = safeget(sp2, 4 );
		return rectangleWithinRectangle( px0, py0, pr0,pr0, nx0, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		r = meterToLon( srid2, r, x0, y0 );
		double nx = safeget(sp2, 3);
		return rectangleWithinCircle( px0, py0, pr0,pr0, nx0, x0, y0, r, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		a = meterToLon( srid2, a, x0, y0 );
		b = meterToLat( srid2, b, x0, y0 );
		double nx = safeget(sp2, 4);
		return rectangleWithinEllipse( px0, py0, pr0,pr0, nx0, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return rectangleWithinPolygon( px0, py0, pr0,pr0,nx0, mk2, sp2, strict );
	}
	return false;
}

bool JagGeo::doSquare3DWithin( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double pr0 = jagatof( sp1[3].c_str() ); 
	double nx0 = safeget( sp1, 4);
	double ny0 = safeget( sp1, 5);

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		// return square3DWithinCube( px0, py0, pz0, pr0, nx0, ny0, x0, y0, z0, r, nx, ny, strict );
		return rectangle3DWithinCube( px0, py0, pz0, pr0,pr0, nx0, ny0, x0, y0, z0, r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		// return square3DWithinBox( px0, py0, pz0, pr0,  nx0, ny0,x0, y0, z0, w,d,h, nx, ny, strict );
		return rectangle3DWithinBox( px0, py0, pz0, pr0,pr0, nx0, ny0,x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		// return square3DWithinSphere( px0, py0, pz0, pr0, nx0, ny0, x, y, z, r, strict );
		return rectangle3DWithinSphere( px0, py0, pz0, pr0,pr0, nx0, ny0, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		// return square3DWithinEllipsoid( px0, py0, pz0, pr0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, strict );
		return rectangle3DWithinEllipsoid( px0, py0, pz0, pr0,pr0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		// return square3DWithinCone( px0, py0, pz0, pr0, nx0, ny0, x0, y0, z0, r,h, nx,ny, strict );
		return rectangle3DWithinCone( px0, py0, pz0, pr0,pr0, nx0, ny0, x0, y0, z0, r,h, nx,ny, strict );
	}
	return false;
}


bool JagGeo::doCubeWithin( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double pr0 = jagatof( sp1[3].c_str() ); 
	double nx0 = safeget(sp1, 4);
	double ny0 = safeget(sp1, 5);

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return boxWithinCube( px0, py0, pz0, pr0,pr0,pr0, nx0, ny0, x0, y0, z0, r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return boxWithinBox( px0, py0, pz0, pr0,pr0,pr0, nx0, ny0, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return boxWithinSphere( px0, py0, pz0, pr0,pr0,pr0, nx0, ny0, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return boxWithinEllipsoid( px0, py0, pz0, pr0,pr0,pr0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return boxWithinCone( px0, py0, pz0, pr0,pr0,pr0, nx0, ny0, x0, y0, z0, r, h, nx,ny, strict );
	}
	return false;
}

// 2D
bool JagGeo::doRectangleWithin( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double a0 = jagatof( sp1[2].c_str() ); 
	double b0 = jagatof( sp1[3].c_str() ); 
	double nx0 = safeget( sp1, 4 );

	a0 = meterToLon( srid2, a0, px0, py0 );
	b0 = meterToLat( srid2, b0, px0, py0 );

	// like point within
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return rectangleWithinTriangle( px0, py0, a0, b0, nx0, x1, y1, x2, y2, x3, y3, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		a = meterToLon( srid2, a, x0, y0 );
		double nx = safeget( sp2, 3 );
		return rectangleWithinSquare( px0, py0, a0, b0, nx0, x0, y0, a, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		a = meterToLon( srid2, a, x0, y0 );
		b = meterToLat( srid2, b, x0, y0 );
		double nx = safeget( sp2, 4 );
		return rectangleWithinRectangle( px0, py0, a0, b0, nx0, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		r = meterToLon( srid2, r, x0, y0 );
		double nx = safeget( sp2, 3 );
		return rectangleWithinCircle( px0, py0, a0, b0, nx0, x0, y0, r, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		a = meterToLon( srid2, a, x0, y0 );
		b = meterToLat( srid2, b, x0, y0 );
		double nx = safeget( sp2, 4 );
		return rectangleWithinEllipse( px0, py0, a0, b0, nx0, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return rectangleWithinPolygon( px0,py0,a0,b0,nx0, mk2, sp2, strict );
	}
	return false;
}

// 3D rectiangle
bool JagGeo::doRectangle3DWithin( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double a0 = jagatof( sp1[3].c_str() ); 
	double b0 = jagatof( sp1[4].c_str() ); 
	double nx0 = safeget( sp1, 5 );
	double ny0 = safeget( sp1, 6 );

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4);
		double ny = safeget( sp2, 5);
		return rectangle3DWithinCube( px0, py0, pz0, a0, b0, nx0, ny0, x0, y0, z0, r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return rectangle3DWithinBox( px0, py0, pz0, a0, b0, nx0, ny0, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return rectangle3DWithinSphere( px0, py0, pz0, a0, b0, nx0, ny0, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return rectangle3DWithinEllipsoid( px0, py0, pz0, a0, b0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5);
		double ny = safeget( sp2, 6);
		return rectangle3DWithinCone( px0, py0, pz0, a0, b0, nx0, ny0, x0, y0, z0, r,h, nx,ny, strict );
	}
	return false;
}

bool JagGeo::doBoxWithin( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double a0 = jagatof( sp1[3].c_str() ); 
	double b0 = jagatof( sp1[4].c_str() ); 
	double c0 = jagatof( sp1[5].c_str() ); 
	double nx0 = safeget( sp1, 6 );
	double ny0 = safeget( sp1, 7 );

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return boxWithinCube( px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return boxWithinBox( px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return boxWithinSphere( px0, py0, pz0, a0, b0, c0, nx0, ny0, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return boxWithinEllipsoid( px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return boxWithinCone( px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, r,h, nx,ny, strict );
	}
	return false;
}


// 3D
bool JagGeo::doCylinderWithin( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double pr0 = jagatof( sp1[3].c_str() ); 
	double c0 = jagatof( sp1[4].c_str() ); 

	double nx0 = safeget(sp1, 5);
	double ny0 = safeget(sp1, 6);

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return cylinderWithinCube( px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return cylinderWithinBox( px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return cylinderWithinSphere( px0, py0, pz0, pr0, c0,  nx0, ny0, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return cylinderWithinEllipsoid( px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return cylinderWithinCone( px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, r,h, nx,ny, strict );
	}
	return false;
}

bool JagGeo::doConeWithin( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double pr0 = jagatof( sp1[3].c_str() ); 
	double c0 = jagatof( sp1[4].c_str() ); 
	double nx0 = safeget(sp1, 5 );
	double ny0 = safeget(sp1, 6 );

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return coneWithinCube( px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return coneWithinBox( px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return coneWithinSphere( px0, py0, pz0, pr0, c0,  nx0, ny0, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return coneWithinEllipsoid( px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return coneWithinCone( px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, r,h, nx,ny, strict );
	}
	return false;
}

// 2D
bool JagGeo::doEllipseWithin( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double a0 = jagatof( sp1[2].c_str() ); 
	double b0 = jagatof( sp1[3].c_str() ); 
	double nx0 = safeget(sp1, 4);

	a0 = meterToLon( srid2, a0, px0, py0 );
	b0 = meterToLat( srid2, b0, px0, py0 );

	// like point within
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return ellipseWithinTriangle( px0, py0, a0, b0, nx0, x1, y1, x2, y2, x3, y3, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3);
		a = meterToLon( srid2, a, x0, y0 );
		double b = meterToLat( srid2, a, x0, y0 );
		//return ellipseWithinSquare( px0, py0, a0, b0, nx0, x0, y0, a, nx, strict );
		return ellipseWithinRectangle( px0, py0, a0, b0, nx0, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		a = meterToLon( srid2, a, x0, y0 );
		b = meterToLat( srid2, b, x0, y0 );
		double nx = safeget( sp2, 4);
		return ellipseWithinRectangle( px0, py0, a0, b0, nx0, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		r = meterToLon( srid2, r, x0, y0 );
		double nx = safeget( sp2, 3);
		return ellipseWithinCircle( px0, py0, a0, b0, nx0, x0, y0, r, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4);
		a = meterToLon( srid2, a, x0, y0 );
		b = meterToLat( srid2, b, x0, y0 );
		return ellipseWithinEllipse( px0, py0, a0, b0, nx0, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return ellipseWithinPolygon( px0, py0, a0, b0, nx0, mk2, sp2, strict );
	}
	return false;
}

// 3D ellipsoid
bool JagGeo::doEllipsoidWithin( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double a0 = jagatof( sp1[3].c_str() ); 
	double b0 = jagatof( sp1[4].c_str() ); 
	double c0 = jagatof( sp1[5].c_str() ); 
	double nx0 = safeget( sp1, 6);
	double ny0 = safeget( sp1, 7);

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4);
		double ny = safeget( sp2, 5);
		return ellipsoidWithinCube( px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return ellipsoidWithinBox( px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return ellipsoidWithinSphere( px0, py0, pz0, a0, b0, c0, nx0, ny0, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return ellipsoidWithinEllipsoid( px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5);
		double ny = safeget( sp2, 6);
		return ellipsoidWithinCone( px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, r,h, nx,ny, strict );
	}
	return false;
}

// 2D triangle within
bool JagGeo::doTriangleWithin( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double x10 = jagatof( sp1[0].c_str() );
	double y10 = jagatof( sp1[1].c_str() );
	double x20 = jagatof( sp1[2].c_str() );
	double y20 = jagatof( sp1[3].c_str() );
	double x30 = jagatof( sp1[4].c_str() );
	double y30 = jagatof( sp1[5].c_str() );

	// like point within
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return triangleWithinTriangle( x10, y10, x20, y20, x30, y30, x1, y1, x2, y2, x3, y3, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3);
		a = meterToLon( srid2, a, x0, y0 );
		double b = meterToLat( srid2, a, x0, y0 );
		// return triangleWithinSquare( x10, y10, x20, y20, x30, y30, x0, y0, a, nx, strict );
		return triangleWithinRectangle( x10, y10, x20, y20, x30, y30, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		a = meterToLon( srid2, a, x0, y0 );
		b = meterToLat( srid2, b, x0, y0 );
		double nx = safeget( sp2, 4);
		return triangleWithinRectangle( x10, y10, x20, y20, x30, y30, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		r = meterToLon( srid2, r, x0, y0 );
		double nx = safeget( sp2, 3);
		return triangleWithinCircle( x10, y10, x20, y20, x30, y30, x0, y0, r, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		a = meterToLon( srid2, a, x0, y0 );
		b = meterToLat( srid2, b, x0, y0 );
		double nx = safeget( sp2, 4);
		return triangleWithinEllipse( x10, y10, x20, y20, x30, y30, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return triangleWithinPolygon( x10, y10, x20, y20, x30, y30, mk2, sp2, strict );
	}
	return false;
}

// 3D  triangle
bool JagGeo::doTriangle3DWithin( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double x10 = jagatof( sp1[0].c_str() );
	double y10 = jagatof( sp1[1].c_str() );
	double z10 = jagatof( sp1[2].c_str() );
	double x20 = jagatof( sp1[3].c_str() );
	double y20 = jagatof( sp1[4].c_str() );
	double z20 = jagatof( sp1[5].c_str() );
	double x30 = jagatof( sp1[6].c_str() );
	double y30 = jagatof( sp1[7].c_str() );
	double z30 = jagatof( sp1[8].c_str() );

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4);
		double ny = safeget( sp2, 5);
		return triangle3DWithinCube( x10,y10,z10,x20,y20,z20,x30,y30,z30, x0, y0, z0, r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return triangle3DWithinBox( x10,y10,z10,x20,y20,z20,x30,y30,z30, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return triangle3DWithinSphere( x10,y10,z10,x20,y20,z20,x30,y30,z30, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return triangle3DWithinEllipsoid( x10,y10,z10,x20,y20,z20,x30,y30,z30, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5);
		double ny = safeget( sp2, 6);
		return triangle3DWithinCone( x10,y10,z10,x20,y20,z20,x30,y30,z30, x0, y0, z0, r,h, nx,ny, strict );
	}
	return false;
}

// 2D line
bool JagGeo::doLineWithin( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double x10 = jagatof( sp1[0].c_str() );
	double y10 = jagatof( sp1[1].c_str() );
	double x20 = jagatof( sp1[2].c_str() );
	double y20 = jagatof( sp1[3].c_str() );

	// like point within
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return lineWithinTriangle( x10, y10, x20, y20, x1, y1, x2, y2, x3, y3, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		a = meterToLon( srid2, a, x0, y0 );
		double nx = safeget( sp2, 3 );
		return lineWithinSquare( x10, y10, x20, y20, x0, y0, a, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING ) {
		return lineWithinLineString( x10, y10, x20, y20, mk2, sp2, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		a = meterToLon( srid2, a, x0, y0 );
		b = meterToLat( srid2, b, x0, y0 );
		double nx = safeget( sp2, 4 );
		return lineWithinRectangle( x10, y10, x20, y20, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		r = meterToLon( srid2, r, x0, y0 );
		double nx = safeget( sp2, 3 );
		return lineWithinCircle( x10, y10, x20, y20, x0, y0, r, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		a = meterToLon( srid2, a, x0, y0 );
		b = meterToLat( srid2, b, x0, y0 );
		double nx = safeget( sp2, 4 );
		return lineWithinEllipse( x10, y10, x20, y20, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return lineWithinPolygon( x10, y10, x20, y20, mk2, sp2, strict );
	}
	return false;
}

bool JagGeo::doLine3DWithin( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double x10 = jagatof( sp1[0].c_str() );
	double y10 = jagatof( sp1[1].c_str() );
	double z10 = jagatof( sp1[2].c_str() );
	double x20 = jagatof( sp1[3].c_str() );
	double y20 = jagatof( sp1[4].c_str() );
	double z20 = jagatof( sp1[5].c_str() );

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return line3DWithinCube( x10,y10,z10,x20,y20,z20, x0, y0, z0, r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return line3DWithinLineString3D( x10,y10,z10,x20,y20,z20, mk2, sp2, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return line3DWithinBox( x10,y10,z10,x20,y20,z20, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return line3DWithinSphere( x10,y10,z10,x20,y20,z20, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return line3DWithinEllipsoid( x10,y10,z10,x20,y20,z20, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return line3DWithinCone( x10,y10,z10,x20,y20,z20, x0, y0, z0, r,h, nx,ny, strict );
	}
	return false;
}

// 2D linestring
bool JagGeo::doLineStringWithin( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, 
								 const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, bool strict )
{
	// like point within
	//prt(("s6683 doLineStringWithin colType2=[%s]\n", colType2.c_str() ));
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return lineStringWithinTriangle( mk1, sp1, x1, y1, x2, y2, x3, y3, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING ) {
		return lineStringWithinLineString( mk1, sp1, mk2, sp2, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		a = meterToLon( srid2, a, x0, y0 );
		double nx = safeget( sp2, 3 );
		//prt(("s0881 lineStringWithinSquare ...\n" ));
		return lineStringWithinSquare( mk1, sp1, x0, y0, a, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		a = meterToLon( srid2, a, x0, y0 );
		b = meterToLat( srid2, b, x0, y0 );
		double nx = safeget( sp2, 4 );
		return lineStringWithinRectangle( mk1, sp1, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		r = meterToLon( srid2, r, x0, y0 );
		double nx = safeget( sp2, 3 );
		return lineStringWithinCircle( mk1, sp1, x0, y0, r, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		a = meterToLon( srid2, a, x0, y0 );
		b = meterToLat( srid2, b, x0, y0 );
		double nx = safeget( sp2, 4 );
		return lineStringWithinEllipse( mk1, sp1, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return lineStringWithinPolygon( mk1, sp1, mk2, sp2, strict );
	}
	return false;
}

bool JagGeo::doLineString3DWithin( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, 
									const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, bool strict )
{
	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return lineString3DWithinCube( mk1, sp1, x0, y0, z0, r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return lineString3DWithinLineString3D( mk1, sp1, mk2, sp2, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return lineString3DWithinBox( mk1, sp1, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return lineString3DWithinSphere( mk1, sp1, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return lineString3DWithinEllipsoid( mk1, sp1, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return lineString3DWithinCone( mk1, sp1, x0, y0, z0, r,h, nx,ny, strict );
	}
	return false;
}

bool JagGeo::doPolygonWithin( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, 
								 const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, bool strict )
{
	/***
	//sp1.print();
	//sp1.printStr();

	//sp2.print();
	//sp2.printStr();
	***/

	/***
	//sp1.print();
	i=0 [_OJAG_=0=test.pol2.po2=PL]
	i=1 [0.0:0.0:500.0:600.0] // bbox
	i=2 [0.0:0.0]
	i=3 [20.0:0.0]
	i=4 [8.0:9.0]
	i=5 [0.0:0.0]
	i=6 [|]
	i=7 [1.0:2.0]
	i=8 [2.0:3.0]
	i=9 [1.0:2.0]
	***/

	// like point within
	prt(("s6683 doPolygonWithin colType2=[%s]\n", colType2.c_str() ));
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return polygonWithinTriangle( mk1, sp1, x1, y1, x2, y2, x3, y3, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		prt(("s6040 JAG_C_COL_TYPE_SQUARE sp2 print():\n"));
		//sp2.print();
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		a = meterToLon( srid2, a, x0, y0 );
		double nx = safeget( sp2, 3 );
		return polygonWithinSquare( mk1, sp1, x0, y0, a, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		a = meterToLon( srid2, a, x0, y0 );
		b = meterToLat( srid2, b, x0, y0 );
		double nx = safeget( sp2, 4 );
		return polygonWithinRectangle( mk1, sp1, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		r = meterToLon( srid2, r, x0, y0 );
		double nx = safeget( sp2, 3 );
		return polygonWithinCircle( mk1, sp1, x0, y0, r, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		a = meterToLon( srid2, a, x0, y0 );
		b = meterToLat( srid2, b, x0, y0 );
		double nx = safeget( sp2, 4 );
		return polygonWithinEllipse( mk1, sp1, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return polygonWithinPolygon( mk1, sp1, mk2, sp2 );
	}
	return false;
}

double JagGeo::doMultiPolygonArea( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1 )
{
	prt(("s7739 doMultiPolygonArea sp1.print():\n" ));
	sp1.print();

   	int start = 0;
   	if ( mk1 == JAG_OJAG ) { start = 1; }
    double dx, dy;
    const char *str;
    char *p;
	bool skip = false;
	double ar, area = 0.0;
	int ring = 0;
	if ( 0 == srid1 ) {
    	JagVector<std::pair<double,double>> vec;
    	for ( int i=start; i < sp1.length(); ++i ) {
    		if ( sp1[i] == "!" || sp1[i] == "|" ) { 
    			ar = computePolygonArea( vec );
				if ( 0 == ring ) {
    				area += ar;
				} else {
    				area -= ar;
				}
				vec.clean();
				if ( sp1[i] == "!" ) { ring = 0;}
				else { ++ring; }
			} else {
        		str = sp1[i].c_str();
        		if ( strchrnum( str, ':') < 1 ) continue;
        		get2double(str, p, ':', dx, dy );
    			vec.append(std::make_pair(dx, dy));
    		}
		}
    
    	if ( vec.size() > 0 ) {
    		ar = computePolygonArea( vec );
			if ( 0 == ring ) {
    			area += ar;
			} else {
    			area -= ar;
			}
    	}
	} else if ( JAG_GEO_WGS84 == srid1 ) {
		const Geodesic& geod = Geodesic::WGS84();
		PolygonArea poly(geod);
		double perim;
		int numPoints = 0;
    	for ( int i=start; i < sp1.length(); ++i ) {
    		if ( sp1[i] == "!" || sp1[i] == "|" ) { 
				poly.Compute( false, true, perim, ar );
				if ( 0 == ring ) {
    				area += ar;
				} else {
    				area -= ar;  // hole
				}
    			poly.Clear(); 
				numPoints = 0;
				if ( sp1[i] == "!" ) { ring = 0;}
				else { ++ring; }
			} else {
        		str = sp1[i].c_str();
        		if ( strchrnum( str, ':') < 1 ) continue;
        		get2double(str, p, ':', dx, dy );
				poly.AddPoint( dx, dy );
				++numPoints;
    		}
		}
    
    	if ( numPoints > 0 ) {
			poly.Compute( false, true, perim, ar );
			if ( 0 == ring ) {
    			area += ar;
			} else {
    			area -= ar;
			}
    	}
	}

	return area;
}


bool JagGeo::doMultiPolygonWithin( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, 
								 const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, bool strict )
{
	// like point within
	prt(("s6683 domultiPolygonWithin colType2=[%s]\n", colType2.c_str() ));
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return multiPolygonWithinTriangle( mk1, sp1, x1, y1, x2, y2, x3, y3, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		prt(("s6040 JAG_C_COL_TYPE_SQUARE sp2 print():\n"));
		//sp2.print();
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		a = meterToLon( srid2, a, x0, y0 );
		double nx = safeget( sp2, 3 );
		return multiPolygonWithinSquare( mk1, sp1, x0, y0, a, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		return multiPolygonWithinRectangle( mk1, sp1, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		r = meterToLon( srid2, r, x0, y0 );
		double nx = safeget( sp2, 3 );
		return multiPolygonWithinCircle( mk1, sp1, x0, y0, r, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		a = meterToLon( srid2, a, x0, y0 );
		b = meterToLat( srid2, b, x0, y0 );
		double nx = safeget( sp2, 4 );
		return multiPolygonWithinEllipse( mk1, sp1, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return multiPolygonWithinPolygon( mk1, sp1, mk2, sp2 );
	}
	return false;
}


bool JagGeo::doPolygon3DWithin( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, 
									const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, bool strict )
{
	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return polygon3DWithinCube( mk1, sp1, x0, y0, z0, r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return polygon3DWithinBox( mk1, sp1, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return polygon3DWithinSphere( mk1, sp1, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return polygon3DWithinEllipsoid( mk1, sp1, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return polygon3DWithinCone( mk1, sp1, x0, y0, z0, r,h, nx,ny, strict );
	}
	return false;
}

bool JagGeo::doMultiPolygon3DWithin( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, 
									const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, bool strict )
{
	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return multiPolygon3DWithinCube( mk1, sp1, x0, y0, z0, r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return multiPolygon3DWithinBox( mk1, sp1, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return multiPolygon3DWithinSphere( mk1, sp1, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return multiPolygon3DWithinEllipsoid( mk1, sp1, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return multiPolygon3DWithinCone( mk1, sp1, x0, y0, z0, r,h, nx,ny, strict );
	}
	return false;
}

////////////////////////////////////// 2D circle /////////////////////////
bool JagGeo::circleWithinTriangle( double px, double py, double pr, double x1, double y1, double x2, double y2, 
								double x3, double y3, bool strict, bool bound )
{
		bool rc;
		rc = pointInTriangle( px, py, x1, y1, x2, y2, x3, y3, strict, bound );
		if ( ! rc ) return false;

		pr = pr*pr;

		double dist = (y2-y1)*px - (x2-x1)*py + x2*y1-y2*x1; 
		dist = dist * dist; 
		double ps = (y2-y1)*(y2-y1) + (x2-x1)*(x2-x1);
		if ( strict ) {
			if ( dist <  ps * pr ) return false;
		} else {
			if ( jagLE(dist, ps * pr ) ) return false;
		}

		dist = (y3-y1)*px - (x3-x1)*py + x3*y1-y3*x1; 
		dist = dist * dist; 
		ps = (y3-y1)*(y3-y1) + (x3-x1)*(x3-x1);
		if ( strict ) {
			if ( dist <  ps * pr ) return false;
		} else {
			if ( jagLE(dist, ps * pr ) ) return false;
		}

		dist = (y3-y2)*px - (x3-x2)*py + x3*y2-y3*x2; 
		dist = dist * dist; 
		ps = (y3-y2)*(y3-y2) + (x3-x2)*(x3-x2);
		if ( strict ) {
			if ( dist <  ps * pr ) return false;
		} else {
			if ( jagLE(dist, ps * pr ) ) return false;
		}
		return true;
}


bool JagGeo::circleWithinEllipse( double px0, double py0, double pr, 
							  double x, double y, double w, double h, double nx, 
							  bool strict, bool bound )
{
	if (  bound2DDisjoint( px0, py0, pr,pr, x, y, w,h ) ) { return false; }

	double px, py;
	transform2DCoordGlobal2Local( x, y, px0, py0, nx, px, py );
	if ( bound ) {
		if ( px-pr < x-w || px+pr > x+w || py-pr < y-h || py+pr > y+h ) return false;
	}
	JagVector<JagPoint2D> vec;
	samplesOn2DCircle( px0, py0, pr, 2*NUM_SAMPLE, vec );
	for ( int i=0; i < vec.size(); ++i ) {
		if ( ! pointWithinEllipse( vec[i].x, vec[i].y, x, y, w, h, nx, strict ) ) {
			return false;
		}
	}
	return true;
}

bool JagGeo::circleWithinPolygon( double px0, double py0, double pr, 
							const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict )
{
	double bbx, bby, rx, ry;
	getPolygonBound( mk2, sp2, bbx, bby, rx, ry );
	if ( bound2DDisjoint( px0, py0, pr,pr, bbx, bby, rx, ry ) ) { return false; }

	JagPolygon pgon;
	int rc = JagParser::addPolygonData( pgon, sp2, false );
	if ( rc < 0 ) return false;

	JagVector<JagPoint2D> vec;
	samplesOn2DCircle( px0, py0, pr, NUM_SAMPLE, vec );
	for ( int i=0; i <vec.size(); ++i ) {
		if ( ! pointWithinPolygon( vec[i].x, vec[i].y,  pgon ) ) {
			return false;
		}
	}
	return true;
}


////////////////////////////////////////// 2D point /////////////////////////////////////////
bool JagGeo::pointWithinPoint( double px, double py, double x1, double y1, bool strict )
{
	if ( strict ) return false;
	if ( jagEQ(px,x1) && jagEQ(py,y1) ) {
		return true;
	}
	return false;
}

bool JagGeo::pointWithinLine( double px, double py, double x1, double y1, double x2, double y2, bool strict )
{
	// check if falls in both ends
	if ( (jagIsZero(px-x1) && jagIsZero(py-y1)) || ( jagIsZero(px-x2) && jagIsZero(py-y2) ) ) {
		if ( strict ) return false;
		else return true;
	}

	// slope of (px,py) and (x1,xy) is same as (x1,y1) and (x2,y2)
	double xmin, ymin, endx, endy;
	if ( x1 < x2 ) {
		xmin = x1; ymin = y1; endx = x2; endy = y2;
	} else {
		xmin = x2; ymin = y2; endx = x1; endy = y1;
	}

	if ( jagEQ(xmin, endx) && jagEQ(xmin, px) ) {
		if ( ymin < py && py < endy ) {
			return true;
		} else {
			return false;
		}
	}

	if ( jagEQ(xmin, endx) && ! jagEQ(xmin, px) ) {
		return false;
	}
	if ( ! jagEQ(xmin, endx) && jagEQ(xmin, px) ) {
		return false;
	}

	if ( jagEQ( (endy-ymin)/(endx-xmin), (py-ymin)/(px-xmin) ) ) {
		return true;
	}

	return false;
}

bool JagGeo::pointWithinLineString( double x, double y, 
									const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict )
{
	int start = 0;
	if ( mk2 == JAG_OJAG ) {
		start = 1;
	}

    double dx1, dy1, dx2, dy2;
    const char *str;
    char *p;
	//prt(("s6790 start=%d len=%d  square: x0=%f y0=%f r=%f\n", start, sp1.length(), x0,y0,r ));
	for ( int i=start; i < sp2.length()-1; ++i ) {
		//prt(("s6658 sp1[%d]=[%s]\n", i, sp1[i].c_str() ));
		str = sp2[i].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx1, dy1 );
		str = sp2[i+1].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx2, dy2 );
		if ( pointWithinLine( x,y, dx1,dy1,dx2,dy2, strict) ) { return true; }
	}
	return false;
}

bool JagGeo::pointWithinRectangle( double px0, double py0, double x0, double y0, 
									double w, double h, double nx, bool strict )
{
	if (  bound2DDisjoint( px0, py0, 0.0,0.0, x0, y0, w,h ) ) { return false; }
    double px, py;
	transform2DCoordGlobal2Local( x0, y0, px0, py0, nx, px, py );

	if ( strict ) {
		if ( px > -w && px < w && py >-h && py < h ) { return true; } 
	} else {
		if ( jagGE(px, -w) && jagLE(px, w) && jagGE(py, -h) && jagLE(py, h ) ) { return true; } 
	}
	return false;
}

bool JagGeo::pointWithinCircle( double px, double py, double x0, double y0, double r, bool strict )
{
	if ( px < x0-r || px > x0+r || py < y0-r || py > y0+r ) return false;

	if ( strict ) {
		if ( ( (px-x0)*(px-x0) + (py-y0)*(py-y0) ) < r*r  ) return true; 
	} else {
		if ( jagLE( (px-x0)*(px-x0)+(py-y0)*(py-y0), r*r ) ) return true;
	}
	return false;
}

bool JagGeo::pointWithinEllipse( double px0, double py0, double x0, double y0, 
									double w, double h, double nx, bool strict )
{
	if ( jagIsZero(w) || jagIsZero(h) ) return false;
	if (  bound2DDisjoint( px0, py0, 0.0,0.0, x0, y0, w,h ) ) { return false; }

    double px, py;
	transform2DCoordGlobal2Local( x0, y0, px0, py0, nx, px, py );

	if ( px < -w || px > w || py < -h || py > h ) return false;
	double f = px*px/(w*w) + py*py/(h*h);
	if ( strict ) {
		if ( f < 1.0 ) return true; 
	} else {
		if ( jagLE(f, 1.0) ) return true; 
	}
	return false;
}

bool JagGeo::pointWithinPolygon( double x, double y, 
								const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict )
{
    const char *str;
    char *p;
	//JagLineString3D linestr;
	JagPolygon pgon;
	int rc;
	if ( mk2 == JAG_OJAG ) {
		//prt(("s8123 JAG_OJAG sp2: prnt\n" ));
		//sp2.print();
		rc = JagParser::addPolygonData( pgon, sp2, false );
	} else {
		// form linesrting3d  from pdata
		p = secondTokenStart( sp2.c_str() );
		//prt(("s8110 p=[%s]\n", p ));
		rc = JagParser::addPolygonData( pgon, p, false, false );
		//prt(("s8390 pointWithinPolygon pgon.print():\n" ));
		//pgon.print();
	}

	if ( rc < 0 ) {
		//prt(("s8112 rc=%d false\n", rc ));
		return false;
	}

   	if ( ! pointWithinPolygon( x, y, pgon.linestr[0] ) ) {
		//prt(("s8113 outer polygon false\n" ));
		return false;
	}

	for ( int i=1; i < pgon.size(); ++i ) {
		// pgon.linestr[i] is JagLineString3D
		if ( pointWithinPolygon(x, y, pgon.linestr[i] ) ) {
			//prt(("s8114 i=%d witin hole false\n", i ));
			return false;
		}
	}

	return true;
}

bool JagGeo::point3DWithinLineString3D( double x, double y, double z, 
									const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict )
{
	int start = 0;
	if ( mk2 == JAG_OJAG ) {
		start = 1;
	}

    double dx1, dy1, dz1, dx2, dy2, dz2;
    const char *str;
    char *p;
	//prt(("s6790 start=%d len=%d  square: x0=%f y0=%f r=%f\n", start, sp1.length(), x0,y0,r ));
	for ( int i=start; i < sp2.length()-1; ++i ) {
		//prt(("s6658 sp1[%d]=[%s]\n", i, sp1[i].c_str() ));
		str = sp2[i].c_str();
		if ( strchrnum( str, ':') < 2 ) continue;
		get3double(str, p, ':', dx1, dy1, dz1 );
		str = sp2[i+1].c_str();
		if ( strchrnum( str, ':') < 2 ) continue;
		get3double(str, p, ':', dx2, dy2, dz2 );
		if ( point3DWithinLine3D( x,y,z, dx1,dy1,dz1,dx2,dy2,dz2, strict) ) { return true; }
	}
	return false;
}

///////////////////////// 3D point ////////////////////////////////////////////////
bool JagGeo::point3DWithinBox( double px0, double py0, double pz0,
								 double x0, double y0, double z0, 
								 double w, double d, double h, double nx, double ny, bool strict )
{
	if (  bound3DDisjoint( px0, py0, pz0, 0.0,0.0,0.0, x0, y0, z0, w,d,h ) ) { return false; }
	double px, py, pz;
	transform3DCoordGlobal2Local( x0,y0,z0, px0,py0,pz0, nx, ny, px, py, pz );

	if ( strict ) {
		if ( px > -w && px < w && py >-d && py < d && pz>-h && pz<h ) { return true; } 
	} else {
		if ( jagGE(px, -w) && jagLE(px, w) 
			 && jagGE(py, -d) && jagLE(py, d )
			 && jagGE(pz, -h) && jagLE(pz, h ) ) { return true; } 
	}
	return false;
}

bool JagGeo::point3DWithinSphere( double px, double py, double pz, 
								  double x, double y, double z, double r, bool strict )
{
	if ( px < x-r || px > x+r || py < y-r || py > y+r || pz<z-r || pz>z+r ) return false;

	if ( strict ) {
		if ( ( (px-x)*(px-x) + (py-y)*(py-y) + (pz-z)*(pz-z) ) < r*r  ) return true; 
	} else {
		if ( jagLE( (px-x)*(px-x)+(py-y)*(py-y)+(pz-z)*(pz-z), r*r ) ) return true;
	}
	return false;
}

bool JagGeo::point3DWithinEllipsoid( double px0, double py0, double pz0,
								 double x0, double y0, double z0, 
								 double a, double b, double c, double nx, double ny, bool strict )
{
	if ( jagIsZero(a) || jagIsZero(b) || jagIsZero(c) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, 0.0,0.0,0.0, x0, y0, z0, a,b,c ) ) { return false; }

	double px, py, pz;
	transform3DCoordGlobal2Local( x0,y0,z0, px0,py0,pz0, nx, ny, px, py, pz );
	if ( ! point3DWithinNormalEllipsoid( px,py,pz, a,b,c, strict ) ) { return false; }
	return true;
}


/////////////////////////////////////// 2D circle ////////////////////////////////////////
bool JagGeo::circleWithinCircle( double px, double py, double pr, double x, double y, double r, bool strict )
{
	if ( px+pr < x-r || px-pr > x+r || py+pr < y-r || py-pr > y+r ) return false;

	double dist2  = (px-x)*(px-x) + (py-y)*(py-y);
	if ( strict ) {
		// strictly inside
		if ( dist2 < fabs(pr-r)*fabs(pr-r) ) return true;
	} else {
		if ( jagLE( dist2,  fabs(pr-r)*fabs(pr-r) ) ) return true;
	}
	return false;
}

bool JagGeo::circleWithinSquare( double px0, double py0, double pr, double x0, double y0, double r, double nx, bool strict )
{
	if ( ! validDirection(nx) ) return false;
	if (  bound2DDisjoint( px0, py0, pr,pr, x0, y0, r,r ) ) { return false; }
    double px, py;
	transform2DCoordGlobal2Local( x0,y0, px0,py0, nx, px, py );

	if ( strict ) {
		if ( px+pr < r && py+pr < r && px-pr > -r && py-pr > -r ) return true;
	} else {
		if ( jagLE(px+pr, r) && jagLE(py+pr, r) && jagGE(px-pr, -r) && jagGE(py-pr, -r) ) return true;
	}
	return false;
}

bool JagGeo::circleWithinRectangle( double px0, double py0, double pr, double x0, double y0, 
									double w, double h, double nx,  bool strict )
{
	if ( ! validDirection(nx) ) return false;
    double px, py;
	transform2DCoordGlobal2Local( x0,y0, px0,py0, nx, px, py );

	if ( strict ) {
		if ( px+pr < w && py+pr < h && px-pr > -w && py-pr > -h ) return true;
	} else {
		if ( jagLE(px+pr, w) && jagLE(py+pr, h) && jagGE(px-pr, -w) && jagGE(py-pr, -h) ) return true;
	}
	return false;
}


//////////////////////////////////////// 3D circle ///////////////////////////////////////
bool JagGeo::circle3DWithinCube( double px0, double py0, double pz0, double pr0, double nx0, double ny0,
							     double x, double y, double z,  double r, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, pr0,pr0,pr0, x, y, z, r,r,r ) ) { return false; }

	JagVector<JagPoint3D> vec;
	samplesOn3DCircle( px0, py0, pz0, pr0, nx0, ny0, NUM_SAMPLE, vec );
	double sq_x, sq_y, sq_z, px, py, pz;
	for ( int i=0; i < vec.size(); ++i ) {
		transform3DCoordLocal2Global( px0, py0, pz0, vec[i].x, vec[i].y, vec[i].z, nx0, ny0, sq_x, sq_y, sq_z );
		transform3DCoordGlobal2Local( x, y, z, sq_x, sq_y, sq_z, nx, ny, px, py, pz );
		// x y z is center of second cube
		// px py pz are within second cube sys
		if ( ! locIn3DCenterBox( px,py,pz, r,r,r, strict ) ) { return false; }
	}
	return true;
}

bool JagGeo::circle3DWithinBox( double px0, double py0, double pz0, double pr0, double nx0, double ny0,
							    double x, double y, double z,  double w, double d, double h, 
								double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, pr0,pr0,pr0, x, y, z, w,d,h ) ) { return false; }

	JagVector<JagPoint3D> vec;
	samplesOn3DCircle( px0, py0, pz0, pr0, nx0, ny0, NUM_SAMPLE, vec );
	double sq_x, sq_y, sq_z, px, py, pz;
	for ( int i=0; i < vec.size(); ++i ) {
		transform3DCoordLocal2Global( px0, py0, pz0, vec[i].x, vec[i].y, vec[i].z, nx0, ny0, sq_x, sq_y, sq_z );
		transform3DCoordLocal2Global( px0, py0, pz0, vec[i].x, vec[i].y, vec[i].z, nx0, ny0, sq_x, sq_y, sq_z );
		transform3DCoordGlobal2Local( x, y, z, sq_x, sq_y, sq_z, nx, ny, px, py, pz );
		if ( ! locIn3DCenterBox( px,py,pz, w,d,h, strict ) ) { return false; }
	}
	return true;
}

bool JagGeo::circle3DWithinSphere( double px0, double py0, double pz0, double pr0, double nx0, double ny0,
								   double x, double y, double z, double r, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, pr0,pr0,pr0, x, y, z, r,r,r ) ) { return false; }
	/***
	if ( px0-pr0 < x-r || px0+pr0 > x+r || py0-pr0 < y-r || py0+pr0 > y+r || pz0-pr0<z-r || pz0+pr0>z+r ) return false;

	if ( strict ) {
		if ( ( (px0-x)*(px0-x) + (py0-y)*(py0-y) + (pz0-z)*(pz0-z) ) < (r-pr0)*(r-pr0)  ) return true; 
	} else {
		if ( jagLE( (px0-x)*(px0-x)+(py0-y)*(py0-y)+(pz0-z)*(pz0-z), (r-pr0)*(r-pr0) ) ) return true;
	}
	return false;
	***/
	JagVector<JagPoint3D> vec;
	samplesOn3DCircle( px0, py0, pz0, pr0, nx0, ny0, NUM_SAMPLE, vec );
	double sq_x, sq_y, sq_z, locx, locy, locz, d2;
	for ( int i=0; i < vec.size(); ++i ) {
		transform3DCoordLocal2Global( px0, py0, pz0, vec[i].x, vec[i].y, vec[i].z, nx0, ny0, sq_x, sq_y, sq_z );
		locx = sq_x - x; locy = sq_y - y; locz = sq_z - z;
		d2 = locx*locx + locy*locy + locz*locz;
		if ( strict ) {
			if ( jagGE(d2, r*r) ) return false;
		} else {
			if ( d2 > r*r ) return false;
		}
	}
	return true;
}


bool JagGeo::circle3DWithinEllipsoid( double px0, double py0, double pz0, double pr0, double nx0, double ny0,
									  double x, double y, double z, double w, double d, double h,
									  double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, pr0,pr0,pr0, x, y, z, w,d,h ) ) { return false; }

	JagVector<JagPoint3D> vec;
	samplesOn3DCircle( px0, py0, pz0, pr0, nx0, ny0, NUM_SAMPLE, vec );
	double sq_x, sq_y, sq_z, locx, locy, locz ;
	for ( int i=0; i < vec.size(); ++i ) {
		transform3DCoordLocal2Global( px0, py0, pz0, vec[i].x, vec[i].y, vec[i].z, nx0, ny0, sq_x, sq_y, sq_z );
		transform3DCoordGlobal2Local( x, y, z, sq_x, sq_y, sq_z, nx, ny, locx, locy, locz );
		if ( ! point3DWithinNormalEllipsoid( locx, locy, locz, w,d,h, strict ) ) { return false; }
	}
	return true;
}


////////////////////////////////////// 3D sphere ////////////////////////////////////////
bool JagGeo::sphereWithinCube(  double px0, double py0, double pz0, double pr0,
						        double x0, double y0, double z0, double r, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound2DDisjoint( px0, py0, pr0,pr0, x0, y0, r,r ) ) { return false; }

    double px, py, pz, x, y, z;
    rotate3DCoordGlobal2Local( px0, py0, pz0, nx, ny, px, py, pz );
    rotate3DCoordGlobal2Local( x0, y0, z0, nx, ny, x, y, z );

	if ( strict ) {
		if ( px-pr0 > x-r && px+pr0 < x+r && py-pr0 >y-r && py+pr0 < y+r && pz-pr0 > z-r && pz+pr0 < z+r ) { return true; } 
	} else {
		if ( jagGE(px-pr0, x-r) && jagLE(px+pr0, x+r) 
		     && jagGE(py-pr0, y-r) && jagLE(py+pr0, y+r )
			 && jagGE(pz-pr0, z-r) && jagLE(pz+pr0, z+r )) { return true; } 
	}
	return false;
}


bool JagGeo::sphereWithinBox(  double px0, double py0, double pz0, double r,
						        double x0, double y0, double z0, 
							    double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx) ) return false;

    double px, py, pz, x, y, z;
    rotate3DCoordGlobal2Local( px0, py0, pz0, nx, ny, px, py, pz );
    rotate3DCoordGlobal2Local( x0, y0, z0, nx, ny, x, y, z );

	if ( strict ) {
		if ( px-r > x-w && px+r < x+w && py-r >y-d && py+r < y+d && pz-r>z-h && pz+r<z+h ) { return true; } 
	} else {
		if ( jagGE(px-r, x-w) && jagLE(px+r, x+w) 
			 && jagGE(py-r, y-d) && jagLE(py+r, y+d )
			 && jagGE(pz-r, z-h) && jagLE(pz+r, z+h ) ) { return true; } 
	}
	return false;
}

bool JagGeo::sphereWithinSphere(  double px, double py, double pz, double pr,
                                double x, double y, double z, double r, bool strict )
{
	if ( px-pr < x-r || px+pr > x+r || py-pr < y-r || py+pr > y+r || pz-pr<z-r || pz+pr>z+r ) return false;

	if ( strict ) {
		if ( ( (px-x)*(px-x) + (py-y)*(py-y) + (pz-z)*(pz-z) ) < (r-pr)*(r-pr)  ) return true; 
	} else {
		if ( jagLE( (px-x)*(px-x)+(py-y)*(py-y)+(pz-z)*(pz-z), (r-pr)*(r-pr) ) ) return true;
	}
	return false;
}

bool JagGeo::sphereWithinEllipsoid(  double px0, double py0, double pz0, double pr,
						        	double x0, double y0, double z0, 
							    	double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx, ny) ) return false;

	if ( jagIsZero(w) || jagIsZero(d) || jagIsZero(h) ) return false;

    double px, py, pz, x, y, z;
    rotate3DCoordGlobal2Local( px0, py0, pz0, nx, ny, px, py, pz );
    rotate3DCoordGlobal2Local( x0, y0, z0, nx, ny, x, y, z );

	if ( px-pr < x-w || px+pr > x+w || py-pr < y-d || py+pr > y+d || pz-pr<z-h || pz+pr>z+h ) return false;
	w -= pr; d -= pr; h -= pr;

	double f = (px-x)*(px-x)/(w*w) + (py-y)*(py-y)/(d*d) + (pz-z)*(pz-z)/(h*h);
	if ( strict ) {
		if ( f < 1.0 ) return true; 
	} else {
		if ( jagLE(f, 1.0) ) return true; 
	}
	return false;
}

//////////////////////////// 2D rectangle //////////////////////////////////////////////////
bool JagGeo::rectangleWithinTriangle( double px0, double py0, double a0, double b0, double nx0,
									double x1, double y1, double x2, double y2, double x3, double y3,
									bool strict )
{
	if ( ! validDirection(nx0) ) return false;
	double X2, Y2, R2x, R2y;
	triangleRegion( x1, y1, x2, y2, x3, y3, X2, Y2, R2x, R2y );
	if (  bound2DDisjoint( px0, py0, a0,b0, X2, Y2, R2x, R2y ) ) { return false; }

	double sq_x1, sq_y1, sq_x2, sq_y2, sq_x3, sq_y3,  sq_x4, sq_y4;
	transform2DCoordLocal2Global( px0, py0, -a0, -b0, nx0, sq_x1, sq_y1 );
	transform2DCoordLocal2Global( px0, py0, -a0, b0, nx0, sq_x2, sq_y2 );
	transform2DCoordLocal2Global( px0, py0, a0, b0, nx0, sq_x3, sq_y3 );
	transform2DCoordLocal2Global( px0, py0, a0, -b0, nx0, sq_x4, sq_y4 );

   	JagPoint2D p1( x1, y1 );
   	JagPoint2D p2( x2, y2 );
   	JagPoint2D p3( x3, y3 );

   	JagPoint2D psq( sq_x1, sq_y1 );
	bool rc = pointWithinTriangle( psq, p1, p2, p3, strict, true );
	if ( ! rc ) return false;

	psq.x = sq_x2; psq.y = sq_y2;
	rc = pointWithinTriangle( psq, p1, p2, p3, strict, true );
	if ( ! rc ) return false;

	psq.x = sq_x3; psq.y = sq_y3;
	rc = pointWithinTriangle( psq, p1, p2, p3, strict, true );
	if ( ! rc ) return false;

	psq.x = sq_x4; psq.y = sq_y4;
	rc = pointWithinTriangle( psq, p1, p2, p3, strict, true );
	if ( ! rc ) return false;

	return true;
}

bool JagGeo::rectangleWithinSquare( double px0, double py0, double a0, double b0, double nx0,
							 	double x0, double y0, double r, double nx, bool strict )

{
	if ( ! validDirection(nx0) ) return false;
	if ( ! validDirection(nx) ) return false;
	if (  bound2DDisjoint( px0, py0, a0,b0, x0, y0, r,r ) ) { return false; }

	double sq_x[4], sq_y[4];
	transform2DCoordLocal2Global( px0, py0, -a0, -b0, nx0, sq_x[0], sq_y[0] );
	transform2DCoordLocal2Global( px0, py0, -a0, b0, nx0, sq_x[1], sq_y[1] );
	transform2DCoordLocal2Global( px0, py0, a0, b0, nx0, sq_x[2], sq_y[2] );
	transform2DCoordLocal2Global( px0, py0, a0, -b0, nx0, sq_x[3], sq_y[3] );
	double loc_x, loc_y;
	for ( int i=0; i < 4; ++i ) {
    	transform2DCoordGlobal2Local( x0, y0, sq_x[i], sq_y[i], nx, loc_x, loc_y );
		if ( ! locIn2DCenterBox( loc_x, loc_y, r, r, strict ) ) { return false; }
	}

	return true;
}

bool JagGeo::rectangleWithinRectangle( double px0, double py0, double a0, double b0, double nx0,
                                	double x0, double y0, double a, double b, double nx, bool strict )
{
	if ( ! validDirection(nx0) ) return false;
	if ( ! validDirection(nx) ) return false;
	if (  bound2DDisjoint( px0, py0, a0,b0, x0, y0, a,b ) ) { return false; }
	double sq_x[4], sq_y[4];
	transform2DCoordLocal2Global( px0, py0, -a0, -b0, nx0, sq_x[0], sq_y[0] );
	transform2DCoordLocal2Global( px0, py0, -a0, b0, nx0, sq_x[1], sq_y[1] );
	transform2DCoordLocal2Global( px0, py0, a0, b0, nx0, sq_x[2], sq_y[2] );
	transform2DCoordLocal2Global( px0, py0, a0, -b0, nx0, sq_x[3], sq_y[3] );

	double loc_x, loc_y;
	for ( int i=0; i < 4; ++i ) {
    	transform2DCoordGlobal2Local( x0, y0, sq_x[i], sq_y[i], nx, loc_x, loc_y );
		if ( ! locIn2DCenterBox( loc_x, loc_y, a, b, strict ) ) { return false; }
	}
	return true;
}

bool JagGeo::rectangleWithinCircle( double px0, double py0, double a0, double b0, double nx0,
                                	double x0, double y0, double r, double nx, bool strict )
{
	if ( ! validDirection(nx0) ) return false;
	if ( ! validDirection(nx) ) return false;
	if (  bound2DDisjoint( px0, py0, a0,b0, x0, y0, r,r ) ) { return false; }
	double sq_x[4], sq_y[4];
	transform2DCoordLocal2Global( px0, py0, -a0, -b0, nx0, sq_x[0], sq_y[0] );
	transform2DCoordLocal2Global( px0, py0, -a0, b0, nx0, sq_x[1], sq_y[1] );
	transform2DCoordLocal2Global( px0, py0, a0, b0, nx0, sq_x[2], sq_y[2] );
	transform2DCoordLocal2Global( px0, py0, a0, -b0, nx0, sq_x[3], sq_y[3] );
	double r2, loc_x, loc_y;
	r2 = r*r;
	for ( int i=0; i < 4; ++i ) {
    	transform2DCoordGlobal2Local( x0, y0, sq_x[i], sq_y[i], nx, loc_x, loc_y );
		if ( strict ) {
			if ( jagGE( jagsq2(loc_x) + jagsq2(loc_y), r2 ) ) return false;
		} else {
			if ( jagsq2(loc_x) + jagsq2(loc_y) > r2 ) return false;
		}
	}
	return true;
}


bool JagGeo::rectangleWithinEllipse( double px0, double py0, double a0, double b0, double nx0,
                                	double x0, double y0, double a, double b, double nx, bool strict )
{
	if ( ! validDirection(nx0) ) return false;
	if ( ! validDirection(nx) ) return false;
	if (  bound2DDisjoint( px0, py0, a0,b0, x0, y0, a,b ) ) { return false; }
	if ( jagIsZero(a) || jagIsZero(b) ) return false;
	double sq_x[4], sq_y[4];
	transform2DCoordLocal2Global( px0, py0, -a0, -b0, nx0, sq_x[0], sq_y[0] );
	transform2DCoordLocal2Global( px0, py0, -a0, b0, nx0, sq_x[1], sq_y[1] );
	transform2DCoordLocal2Global( px0, py0, a0, b0, nx0, sq_x[2], sq_y[2] );
	transform2DCoordLocal2Global( px0, py0, a0, -b0, nx0, sq_x[3], sq_y[3] );
	double f, loc_x, loc_y;
	for ( int i=0; i < 4; ++i ) {
    	transform2DCoordGlobal2Local( x0, y0, sq_x[i], sq_y[i], nx, loc_x, loc_y );
		f = jagsq2(loc_x)/(a*a) + jagsq2(loc_y)/(b*b);
		if ( strict ) {
			if ( jagGE(f, 1.0) ) return false;
		} else {
			if ( f > 1.0 ) return false; 
		}
	}
	return true;
}

bool JagGeo::rectangleWithinPolygon( double px0, double py0, double a0, double b0, double nx0,
				const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict )
{
	if ( ! validDirection(nx0) ) return false;
	if ( jagIsZero(a0) || jagIsZero(b0) ) return false;
	double bbx, bby, rx, ry;
	getPolygonBound( mk2, sp2, bbx, bby, rx, ry );
	if (  bound2DDisjoint( px0, py0, a0,b0, bbx, bby, rx, ry ) ) { return false; }
	double sq_x[4], sq_y[4];
	transform2DCoordLocal2Global( px0, py0, -a0, -b0, nx0, sq_x[0], sq_y[0] );
	transform2DCoordLocal2Global( px0, py0, -a0, b0, nx0, sq_x[1], sq_y[1] );
	transform2DCoordLocal2Global( px0, py0, a0, b0, nx0, sq_x[2], sq_y[2] );
	transform2DCoordLocal2Global( px0, py0, a0, -b0, nx0, sq_x[3], sq_y[3] );
	JagPolygon pgon;
	int rc = JagParser::addPolygonData( pgon, sp2, false );
	if ( rc < 0 ) return false;
	for ( int i=0; i < 4; ++i ) {
		if ( ! pointWithinPolygon( sq_x[i], sq_y[i], pgon ) ) {
			return false;
		}
	}
	return true;
}


//////////////////////////// 2D line  //////////////////////////////////////////////////
bool JagGeo::lineWithinTriangle( double x10, double y10, double x20, double y20, 
			                         double x1, double y1, double x2, double y2, double x3, double y3, bool strict )

{
   	JagPoint2D p10( x10, y10 );
   	JagPoint2D p20( x20, y20 );

   	JagPoint2D p1( x1, y1 );
   	JagPoint2D p2( x2, y2 );
   	JagPoint2D p3( x3, y3 );

	bool rc = pointWithinTriangle( p10, p1, p2, p3, strict, true );
	if ( ! rc ) return false;

	rc = pointWithinTriangle( p20, p1, p2, p3, strict, true );
	if ( ! rc ) return false;

	return true;
}

bool JagGeo::lineWithinLineString( double x10, double y10, double x20, double y20,
 								   const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict )
{
	// 2 points are some two neighbor points in sp2
	int start = 0;
	if ( mk2 == JAG_OJAG ) {
		start = 1;
	}

    double dx1, dy1, dx2, dy2;
    const char *str;
    char *p;
	//prt(("s6390 start=%d len=%d  square: x0=%f y0=%f r=%f\n", start, sp1.length(), x0,y0,r ));
	for ( int i=start; i < sp2.length()-1; ++i ) {
		str = sp2[i].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx1, dy1 );
		str = sp2[i+1].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx2, dy2 );

		if ( jagEQ(x10, dx1) && jagEQ(y10, dy1) && jagEQ(x20, dx2) && jagEQ(y20, dy2) ) {
			return true;
		}
		if ( jagEQ(x20, dx1) && jagEQ(y20, dy1) && jagEQ(x10, dx2) && jagEQ(y10, dy2) ) {
			return true;
		}
	}
	return false;
}


bool JagGeo::lineWithinSquare( double x10, double y10, double x20, double y20, 
                           double x0, double y0, double r, double nx, bool strict )
{
	if ( ! validDirection(nx) ) return false;
	double sq_x[2], sq_y[2];
	sq_x[0] = x10; sq_y[0] = y10;
	sq_x[1] = x20; sq_y[1] = y20;
	double loc_x, loc_y;
	for ( int i=0; i < 2; ++i ) {
    	transform2DCoordGlobal2Local( x0, y0, sq_x[i], sq_y[i], nx, loc_x, loc_y );
		if ( ! locIn2DCenterBox( loc_x, loc_y, r,r, strict ) ) { return false; }
	}
	return true;
}

bool JagGeo::lineWithinRectangle( double x10, double y10, double x20, double y20, 
                                         double x0, double y0, double a, double b, double nx, bool strict )
{
	if ( ! validDirection(nx) ) return false;
	double sq_x[2], sq_y[2];
	sq_x[0] = x10; sq_y[0] = y10;
	sq_x[1] = x20; sq_y[1] = y20;
	double loc_x, loc_y;
	for ( int i=0; i < 2; ++i ) {
    	transform2DCoordGlobal2Local( x0, y0, sq_x[i], sq_y[i], nx, loc_x, loc_y );
		if ( ! locIn2DCenterBox( loc_x, loc_y, a,b, strict ) ) { return false; }
	}
	return true;
}

bool JagGeo::lineWithinCircle( double x10, double y10, double x20, double y20, 
								   double x0, double y0, double r, double nx, bool strict )

{
	if ( ! validDirection(nx) ) return false;
	double sq_x[2], sq_y[2];
	sq_x[0] = x10; sq_y[0] = y10;
	sq_x[1] = x20; sq_y[1] = y20;
	double loc_x, loc_y;
	for ( int i=0; i < 2; ++i ) {
    	transform2DCoordGlobal2Local( x0, y0, sq_x[i], sq_y[i], nx, loc_x, loc_y );
		if ( ! pointWithinCircle( loc_x, loc_y, 0.0, 0.0, r, strict ) ) { return false; }
	}
	return true;
}


bool JagGeo::lineWithinEllipse( double x10, double y10, double x20, double y20, 
									double x0, double y0, double a, double b, double nx, bool strict )

{
	if ( ! validDirection(nx) ) return false;
	if ( jagIsZero(a) || jagIsZero(b) ) return false;

	double tri_x[2], tri_y[2];
	tri_x[0] = x10; tri_y[0] = y10;
	tri_x[1] = x20; tri_y[1] = y20;
	double loc_x, loc_y;
	for ( int i=0; i < 2; ++i ) {
    	transform2DCoordGlobal2Local( x0, y0, tri_x[i], tri_y[i], nx, loc_x, loc_y );
		if ( ! pointWithinNormalEllipse( loc_x, loc_y, a, b, strict ) ) { return false; }
	}
	return true;
}

bool JagGeo::lineWithinPolygon( double x10, double y10, double x20, double y20,
								const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict )
{
	double bbx, bby, rx, ry;
	getPolygonBound( mk2, sp2, bbx, bby, rx, ry );
	if ( bound2DLineBoxDisjoint( x10, y10, x20, y20, bbx, bby, rx, ry ) ) {
		return false;
	}

    const char *str;
    char *p;
	//JagLineString3D linestr;
	JagPolygon pgon;
	int rc;
	if ( mk2 == JAG_OJAG ) {
		rc = JagParser::addPolygonData( pgon, sp2, false );
	} else {
		// form linesrting3d  from pdata
		p = secondTokenStart( sp2.c_str() );
		rc = JagParser::addPolygonData( pgon, p, false, false );
	}
	if ( rc < 0 ) return false;

   	if ( ! pointWithinPolygon( x10, y10, pgon.linestr[0] ) ) { return false; }
   	if ( ! pointWithinPolygon( x20, y20, pgon.linestr[0] ) ) { return false; }
	for ( int i=1; i < pgon.size(); ++i ) {
		if ( pointWithinPolygon(x10, y10, pgon.linestr[i] ) ) {
			return false;
		}
		if ( pointWithinPolygon(x20, y20, pgon.linestr[i] ) ) {
			return false;
		}
	}

	return true;
}


//////////////////////////// 2D linestring  //////////////////////////////////////////////////
bool JagGeo::lineStringWithinTriangle(  const AbaxDataString &mk1, const JagStrSplit &sp1,
			                         double x1, double y1, double x2, double y2, double x3, double y3, bool strict )

{
	double trix, triy, rx, ry;
	triangleRegion(x1,y1, x2,y2, x3,y3, trix, triy, rx, ry );
	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  trix, triy, rx, ry ) ) {
			return false;
		}
		start = 1;
	}

    JagPoint2D p1( x1, y1 );
    JagPoint2D p2( x2, y2 );
    JagPoint2D p3( x3, y3 );
	bool rc;
	double dx, dy;
	const char *str;
	char *p;
	for ( int i=start; i < sp1.length(); ++i ) {
    	// line: x10, y10 --> x20, y20 
		str = sp1[i].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx, dy );
       	JagPoint2D pt( dx, dy );
    	rc = pointWithinTriangle( pt, p1, p2, p3, strict, true );
    	if ( ! rc ) return false;
	}

	return true;
}

bool JagGeo::lineStringWithinLineString(  const AbaxDataString &mk1, const JagStrSplit &sp1,
											const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict )
{
	int start1 = 0;
	double bbx1, bby1, brx1, bry1;
	if ( mk1 == JAG_OJAG ) {
		boundingBoxRegion(sp1[0], bbx1, bby1, brx1, bry1 );
		start1 = 1;
	}

	int start2 = 0;
	double bbx2, bby2, brx2, bry2;
	if ( mk2 == JAG_OJAG ) {
		boundingBoxRegion(sp2[0], bbx2, bby2, brx2, bry2 );
		start2 = 1;
	}

	if ( bound2DDisjoint( bbx1, bby1, brx1, bry1,  bbx2, bby2, brx2, bry2 ) ) {
		return false;
	}

	// assume sp1 has fewer lines than sp2
	if ( strict ) {
		if ( sp1.length() - start1 >= sp2.length() - start2 ) return false;
	} else {
		if ( sp1.length() - start1 > sp2.length() - start2 ) return false;
	}

	int rc = KMPPointsWithin( sp1, start1, sp2, start2 );
	if ( rc < 0 ) return false;
	return true;
}


bool JagGeo::lineStringWithinSquare( const AbaxDataString &mk1, const JagStrSplit &sp1,
                           			 double x0, double y0, double r, double nx, bool strict )
{
	prt(("s6724 lineStringWithinSquare nx=%f ...\n", nx ));
	if ( ! validDirection(nx) ) return false;
	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  x0, y0, r, r ) ) {
			prt(("s6770 false\n" ));
			return false;
		}
		start = 1;
	}

	double loc_x, loc_y;
    double dx, dy;
    const char *str;
    char *p;
	prt(("s6590 start=%d len=%d  square: x0=%f y0=%f r=%f\n", start, sp1.length(), x0,y0,r ));
	for ( int i=start; i < sp1.length(); ++i ) {
		//prt(("s6658 sp1[%d]=[%s]\n", i, sp1[i].c_str() ));
		str = sp1[i].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx, dy );
    	transform2DCoordGlobal2Local( x0, y0, dx, dy, nx, loc_x, loc_y );
		prt(("s6783 dx=%f dy=%f loc_x=%f loc_y=%f r=%f\n", dx, dy, loc_x, loc_y, r ));
		if ( ! locIn2DCenterBox( loc_x, loc_y, r,r, strict ) ) { 
			prt(("s8630 locIn2DCenterBox false, return false\n" ));
			return false; 
		}
	}
	return true;
}

bool JagGeo::lineStringWithinRectangle( const AbaxDataString &mk1, const JagStrSplit &sp1,
                                        double x0, double y0, double a, double b, double nx, bool strict )
{
	if ( ! validDirection(nx) ) return false;

	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  x0, y0, a, b ) ) {
			return false;
		}
		start = 1;
	}
	
	double loc_x, loc_y;
    double dx, dy;
    const char *str;
    char *p;
	for ( int i=start; i < sp1.length(); ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 1 ) continue;
        get2double(str, p, ':', dx, dy );
    	transform2DCoordGlobal2Local( x0, y0, dx, dy, nx, loc_x, loc_y );
		if ( ! locIn2DCenterBox( loc_x, loc_y, a,b, strict ) ) { return false; }
	}
	return true;

}

bool JagGeo::lineStringWithinCircle( const AbaxDataString &mk1, const JagStrSplit &sp1,
								     double x0, double y0, double r, double nx, bool strict )

{
	if ( ! validDirection(nx) ) return false;

	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  x0, y0, r, r ) ) {
			return false;
		}
		start = 1;
	}
	
	double loc_x, loc_y;
	double dx, dy;
	const char *str;
	char *p;
	for ( int i=start; i < sp1.length(); ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 1 ) continue;
        get2double(str, p, ':', dx, dy );
    	transform2DCoordGlobal2Local( x0, y0, dx, dy, nx, loc_x, loc_y );
		if ( ! pointWithinCircle( loc_x, loc_y, 0.0, 0.0, r, strict ) ) { return false; }
	}
	return true;
}


bool JagGeo::lineStringWithinEllipse( const AbaxDataString &mk1, const JagStrSplit &sp1,
									double x0, double y0, double a, double b, double nx, bool strict )

{
	if ( ! validDirection(nx) ) return false;
	if ( jagIsZero(a) || jagIsZero(b) ) return false;

	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  x0, y0, a, b ) ) {
			return false;
		}
		start = 1;
	}
	
	double loc_x, loc_y;
    double dx, dy;
    const char *str;
    char *p;
	for ( int i=start; i < sp1.length(); ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 1 ) continue;
        get2double(str, p, ':', dx, dy );
    	transform2DCoordGlobal2Local( x0, y0, dx, dy, nx, loc_x, loc_y );
		if ( ! pointWithinNormalEllipse( loc_x, loc_y, a, b, strict ) ) { return false; }
	}
	return true;
}

bool JagGeo::lineStringWithinPolygon( const AbaxDataString &mk1, const JagStrSplit &sp1,
									  const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict )
{

	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx1, bby1, brx1, bry1;
		boundingBoxRegion(sp1[0], bbx1, bby1, brx1, bry1 );

		double bbx2, bby2, rx2, ry2;
		getPolygonBound( mk2, sp2, bbx2, bby2, rx2, ry2 );

		if ( bound2DDisjoint( bbx1, bby1, brx1, bry1,  bbx2, bby2, rx2, ry2 ) ) {
			return false;
		}
		start = 1;
	}

	JagPolygon pgon;
	int rc = JagParser::addPolygonData( pgon, sp2, false );
	if ( rc < 0 ) return false;

    double dx, dy;
    const char *str;
    char *p;
	for ( int i=start; i < sp1.length(); ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 1 ) continue;
        get2double(str, p, ':', dx, dy );
		if ( ! pointWithinPolygon( dx, dy, pgon ) ) {
			return false;
		}
	}
	return true;
}


//////////////////////////// 2D polygon  //////////////////////////////////////////////////
bool JagGeo::polygonWithinTriangle(  const AbaxDataString &mk1, const JagStrSplit &sp1,
			                         double x1, double y1, double x2, double y2, double x3, double y3, bool strict )

{
	double trix, triy, rx, ry;
	triangleRegion(x1,y1, x2,y2, x3,y3, trix, triy, rx, ry );
	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  trix, triy, rx, ry ) ) {
			return false;
		}
		start = 1;
	}

    JagPoint2D p1( x1, y1 );
    JagPoint2D p2( x2, y2 );
    JagPoint2D p3( x3, y3 );
	bool rc;
	double dx, dy;
	const char *str;
	char *p;
	for ( int i=start; i < sp1.length(); ++i ) {
    	// line: x10, y10 --> x20, y20 
		if ( sp1[i] == "|" || sp1[i] == "!" ) break;
		str = sp1[i].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx, dy );
       	JagPoint2D pt( dx, dy );
    	rc = pointWithinTriangle( pt, p1, p2, p3, strict, true );
    	if ( ! rc ) return false;
	}

	return true;
}

bool JagGeo::polygonWithinSquare( const AbaxDataString &mk1, const JagStrSplit &sp1,
                           			 double x0, double y0, double r, double nx, bool strict )
{
	prt(("s6724 polygonWithinSquare nx=%f ...\n", nx ));
	if ( ! validDirection(nx) ) return false;
	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  x0, y0, r, r ) ) {
			prt(("s6770 false\n" ));
			return false;
		}
		start = 1;
	}
	//prt(("s8120 polygonWithinSquare sp1:\n" ));
	//sp1.print();

	double loc_x, loc_y;
    double dx, dy;
    const char *str;
    char *p;
	prt(("s6790 start=%d len=%d  square: x0=%f y0=%f r=%f\n", start, sp1.length(), x0,y0,r ));
	for ( int i=start; i < sp1.length(); ++i ) {
		//prt(("s6658 sp1[%d]=[%s]\n", i, sp1[i].c_str() ));
		if ( sp1[i] == "|" || sp1[i] == "!" ) break;
		str = sp1[i].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx, dy );
    	transform2DCoordGlobal2Local( x0, y0, dx, dy, nx, loc_x, loc_y );
		prt(("s6783 dx=%f dy=%f loc_x=%f loc_y=%f r=%f\n", dx, dy, loc_x, loc_y, r ));
		if ( ! locIn2DCenterBox( loc_x, loc_y, r,r, strict ) ) { 
			prt(("s8630 locIn2DCenterBox false, return false\n" ));
			return false; 
		}
	}
	return true;
}

bool JagGeo::polygonWithinRectangle( const AbaxDataString &mk1, const JagStrSplit &sp1,
                                        double x0, double y0, double a, double b, double nx, bool strict )
{
	if ( ! validDirection(nx) ) return false;

	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  x0, y0, a, b ) ) {
			return false;
		}
		start = 1;
	}
	
	double loc_x, loc_y;
    double dx, dy;
    const char *str;
    char *p;
	for ( int i=start; i < sp1.length(); ++i ) {
		if ( sp1[i] == "|" || sp1[i] == "!" ) break;
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 1 ) continue;
        get2double(str, p, ':', dx, dy );
    	transform2DCoordGlobal2Local( x0, y0, dx, dy, nx, loc_x, loc_y );
		if ( ! locIn2DCenterBox( loc_x, loc_y, a,b, strict ) ) { return false; }
	}
	return true;

}

bool JagGeo::polygonWithinCircle( const AbaxDataString &mk1, const JagStrSplit &sp1,
								     double x0, double y0, double r, double nx, bool strict )

{
	if ( ! validDirection(nx) ) return false;

	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  x0, y0, r, r ) ) {
			return false;
		}
		start = 1;
	}
	
	double loc_x, loc_y;
	double dx, dy;
	const char *str;
	char *p;
	for ( int i=start; i < sp1.length(); ++i ) {
		if ( sp1[i] == "|" || sp1[i] == "!" ) break;
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 1 ) continue;
        get2double(str, p, ':', dx, dy );
    	transform2DCoordGlobal2Local( x0, y0, dx, dy, nx, loc_x, loc_y );
		if ( ! pointWithinCircle( loc_x, loc_y, 0.0, 0.0, r, strict ) ) { return false; }
	}
	return true;
}


bool JagGeo::polygonWithinEllipse( const AbaxDataString &mk1, const JagStrSplit &sp1,
									double x0, double y0, double a, double b, double nx, bool strict )

{
	if ( ! validDirection(nx) ) return false;
	if ( jagIsZero(a) || jagIsZero(b) ) return false;

	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  x0, y0, a, b ) ) {
			return false;
		}
		start = 1;
	}
	
	double loc_x, loc_y;
    double dx, dy;
    const char *str;
    char *p;
	for ( int i=start; i < sp1.length(); ++i ) {
		if ( sp1[i] == "|" || sp1[i] == "!" ) break;
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 1 ) continue;
        get2double(str, p, ':', dx, dy );
    	transform2DCoordGlobal2Local( x0, y0, dx, dy, nx, loc_x, loc_y );
		if ( ! pointWithinNormalEllipse( loc_x, loc_y, a, b, strict ) ) { return false; }
	}
	return true;
}

// first polygon is column, second is _CJAG_ 
// O - O  O - C   C - O   no C-C
bool JagGeo::polygonWithinPolygon( const AbaxDataString &mk1, const JagStrSplit &sp1, 
								   const AbaxDataString &mk2, const JagStrSplit &sp2 )
{
	if ( mk1 == JAG_CJAG && mk2 == JAG_CJAG ) return false;

	int start1 = 0;
	int start2 = 0;
	double bbx1, bby1, brx1, bry1;
	double bbx2, bby2, brx2, bry2;
	double xmin, ymin, xmax, ymax;
   	const char *str;
   	double dx, dy;
	char *p, *pdata;

	xmin = ymin = LONG_MAX; xmax = ymax = LONG_MIN;

	int rc;
	if ( mk1 == JAG_OJAG ) {
		start1 = 1;
	} else {
		pdata = secondTokenStart( sp1.c_str() );
		if ( ! pdata ) return false;
	}
	getPolygonBound( mk1, sp1, bbx1, bby1, brx1, bry1 );

	if ( mk2 == JAG_OJAG ) {
		boundingBoxRegion(sp2[0], bbx2, bby2, brx2, bry2 );
		start2 = 1;
	} else {
		pdata = secondTokenStart( sp2.c_str() );
		if ( ! pdata ) return false;
	}
	getPolygonBound( mk2, sp2, bbx2, bby2, brx2, bry2 );

	if ( bound2DDisjoint( bbx1, bby1, brx1, bry1,  bbx2, bby2, brx2, bry2 ) ) {
		prt(("s7581 bound2DDisjoint two polygon not within\n" ));
		return false;
	}


	if (   mk1 == JAG_OJAG  &&  mk2 == JAG_CJAG ) {
    	JagPolygon pgon;  // each polygon can have multiple linestrings
    	rc = JagParser::addPolygonData( pgon, pdata, true, false );
    	//prt(("s3388 addPolygonData pgon: print():\n" ));
    	//pgon.print();
    
    	if ( rc <  0 ) { return false; }
    	if ( pgon.size() < 1 ) { return false; }
    
    	// sp1 array:  "bbox  x:y x:y  ... | x:y  x:y ...| ..." sp1: start=1 skip '|' and '!'
    	// sp2 cstr:  ( ( x y, x y, ...), ( ... ), (...) )
    	// pgon has sp2 data parsed
    	// check first polygon only for now
    	for ( int i=start1; i < sp1.length(); ++i ) {
    		if ( sp1[i] == "|" || sp1[i] == "!" ) break;
    		str = sp1[i].c_str();
    		if ( strchrnum( str, ':') < 1 ) continue;
    		get2double(str, p, ':', dx, dy );
    		if ( ! pointWithinPolygon( dx, dy, pgon.linestr[0] ) ) {
    			return false;
    		}
    	}
    	return true;
	} else if (  mk1 == JAG_OJAG  &&  mk2 == JAG_OJAG ) {
		// get a linestring from cords in sp2
		JagLineString3D linestr;
		for ( int i=start2; i < sp2.length(); ++i ) {
			if ( sp2[i] == "|" || sp2[i] == "!" ) break;
    		str = sp2[i].c_str();
    		if ( strchrnum( str, ':') < 1 ) continue;
    		get2double(str, p, ':', dx, dy );
			JagPoint2D pt(dx,dy);
			linestr.add(pt);
		}

    	for ( int i=start1; i < sp1.length(); ++i ) {
    		if ( sp1[i] == "|" || sp1[i] == "!" ) break;
    		str = sp1[i].c_str();
    		if ( strchrnum( str, ':') < 1 ) continue;
    		get2double(str, p, ':', dx, dy );
    		if ( ! pointWithinPolygon( dx, dy, linestr ) ) {
    			return false;
    		}
    	}
    	return true;
	} else if (  mk1 == JAG_CJAG  &&  mk2 == JAG_OJAG  ) {
    	JagPolygon pgon;  // each polygon can have multiple linestrings
    	rc = JagParser::addPolygonData( pgon, pdata, true, false );

    	//prt(("s3338 addPolygonData pgon: print():\n" ));
    	//pgon.print();
    
    	if ( rc <  0 ) { return false; }
    	if ( pgon.size() < 1 ) { return false; }
    
    	// sp1 array:  "bbox  x:y x:y  ... | x:y  x:y ...| ..." sp1: start=1 skip '|' and '!'
    	// sp2 cstr:  ( ( x y, x y, ...), ( ... ), (...) )
    	// pgon has sp2 data parsed
    	// check first polygon only for now
    	for ( int i=start2; i < sp2.length(); ++i ) {
    		if ( sp2[i] == "|" || sp2[i] == "!" ) break;
    		str = sp2[i].c_str();
    		if ( strchrnum( str, ':') < 1 ) continue;
    		get2double(str, p, ':', dx, dy );
    		if ( ! pointWithinPolygon( dx, dy, pgon.linestr[0] ) ) {
    			return false;
    		}
    	}
    	return true;
	} else {
    	return false;
	}
}


//////////////////////////// 2D multipolygon  //////////////////////////////////////////////////
bool JagGeo::multiPolygonWithinTriangle(  const AbaxDataString &mk1, const JagStrSplit &sp1,
			                         double x1, double y1, double x2, double y2, double x3, double y3, bool strict )

{
	double trix, triy, rx, ry;
	triangleRegion(x1,y1, x2,y2, x3,y3, trix, triy, rx, ry );
	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  trix, triy, rx, ry ) ) {
			return false;
		}
		start = 1;
	}

    JagPoint2D p1( x1, y1 );
    JagPoint2D p2( x2, y2 );
    JagPoint2D p3( x3, y3 );
	bool rc;
	double dx, dy;
	const char *str;
	char *p;
	bool skip = false;
	for ( int i=start; i < sp1.length(); ++i ) {
		if ( sp1[i] == "|" ) {  skip = true; }
		else if ( sp1[i] == "!" ) {  skip = false; }
		else {
			if ( skip ) continue;
    		str = sp1[i].c_str();
    		if ( strchrnum( str, ':') < 1 ) continue;
    		get2double(str, p, ':', dx, dy );
           	JagPoint2D pt( dx, dy );
        	rc = pointWithinTriangle( pt, p1, p2, p3, strict, true );
        	if ( ! rc ) return false;
    	}
    }
   	return true;
}
    
bool JagGeo::multiPolygonWithinSquare( const AbaxDataString &mk1, const JagStrSplit &sp1,
                               			 double x0, double y0, double r, double nx, bool strict )
{
   	prt(("s6724 multiPolygonWithinSquare nx=%f ...\n", nx ));
   	if ( ! validDirection(nx) ) return false;
   	int start = 0;
   	if ( mk1 == JAG_OJAG ) {
   		double bbx, bby, brx, bry;
   		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  x0, y0, r, r ) ) {
			prt(("s6770 false\n" ));
			return false;
		}
		start = 1;
	}

	double loc_x, loc_y;
    double dx, dy;
    const char *str;
    char *p;
	bool skip = false;
	prt(("s6790 start=%d len=%d  square: x0=%f y0=%f r=%f\n", start, sp1.length(), x0,y0,r ));
	for ( int i=start; i < sp1.length(); ++i ) {
		if ( sp1[i] == "|" ) {  skip = true; }
		else if ( sp1[i] == "!" ) {  skip = false; }
		else {
			if ( skip ) continue;
    		str = sp1[i].c_str();
    		if ( strchrnum( str, ':') < 1 ) continue;
    		get2double(str, p, ':', dx, dy );
        	transform2DCoordGlobal2Local( x0, y0, dx, dy, nx, loc_x, loc_y );
    		prt(("s6783 dx=%f dy=%f loc_x=%f loc_y=%f r=%f\n", dx, dy, loc_x, loc_y, r ));
    		if ( ! locIn2DCenterBox( loc_x, loc_y, r,r, strict ) ) { 
    			prt(("s8630 locIn2DCenterBox false, return false\n" ));
    			return false; 
    		}
		}
	}
	return true;
}

bool JagGeo::multiPolygonWithinRectangle( const AbaxDataString &mk1, const JagStrSplit &sp1,
                                        double x0, double y0, double a, double b, double nx, bool strict )
{
	if ( ! validDirection(nx) ) return false;

	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  x0, y0, a, b ) ) {
			return false;
		}
		start = 1;
	}
	
	double loc_x, loc_y;
    double dx, dy;
    const char *str;
    char *p;
	bool skip = false;
	for ( int i=start; i < sp1.length(); ++i ) {
		if ( sp1[i] == "|" ) {  skip = true; }
		else if ( sp1[i] == "!" ) {  skip = false; }
		else {
			if ( skip ) continue;
        	str = sp1[i].c_str();
        	if ( strchrnum( str, ':') < 1 ) continue;
        	get2double(str, p, ':', dx, dy );
    		transform2DCoordGlobal2Local( x0, y0, dx, dy, nx, loc_x, loc_y );
			if ( ! locIn2DCenterBox( loc_x, loc_y, a,b, strict ) ) { return false; }
		}
	}
	return true;
}

bool JagGeo::multiPolygonWithinCircle( const AbaxDataString &mk1, const JagStrSplit &sp1,
								     double x0, double y0, double r, double nx, bool strict )

{
	if ( ! validDirection(nx) ) return false;

	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  x0, y0, r, r ) ) {
			return false;
		}
		start = 1;
	}
	
	double loc_x, loc_y;
	double dx, dy;
	const char *str;
	char *p;
	bool skip = false;
	for ( int i=start; i < sp1.length(); ++i ) {
		if ( sp1[i] == "|" ) {  skip = true; }
		else if ( sp1[i] == "!" ) {  skip = false; }
		else {
			if ( skip ) continue;
            str = sp1[i].c_str();
            if ( strchrnum( str, ':') < 1 ) continue;
            get2double(str, p, ':', dx, dy );
        	transform2DCoordGlobal2Local( x0, y0, dx, dy, nx, loc_x, loc_y );
    		if ( ! pointWithinCircle( loc_x, loc_y, 0.0, 0.0, r, strict ) ) { return false; }
		}
	}
	return true;
}


bool JagGeo::multiPolygonWithinEllipse( const AbaxDataString &mk1, const JagStrSplit &sp1,
									double x0, double y0, double a, double b, double nx, bool strict )

{
	if ( ! validDirection(nx) ) return false;
	if ( jagIsZero(a) || jagIsZero(b) ) return false;

	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  x0, y0, a, b ) ) {
			return false;
		}
		start = 1;
	}
	
	double loc_x, loc_y;
    double dx, dy;
    const char *str;
    char *p;
	bool skip = false;
	for ( int i=start; i < sp1.length(); ++i ) {
		if ( sp1[i] == "|" ) {  skip = true; }
		else if ( sp1[i] == "!" ) {  skip = false; }
		else {
			if ( skip ) continue;
            str = sp1[i].c_str();
            if ( strchrnum( str, ':') < 1 ) continue;
            get2double(str, p, ':', dx, dy );
        	transform2DCoordGlobal2Local( x0, y0, dx, dy, nx, loc_x, loc_y );
    		if ( ! pointWithinNormalEllipse( loc_x, loc_y, a, b, strict ) ) { return false; }
		}
	}
	return true;
}

// first polygon is column, second is _CJAG_ 
// O - O  O - C   C - O   no C-C
bool JagGeo::multiPolygonWithinPolygon( const AbaxDataString &mk1, const JagStrSplit &sp1, 
								   const AbaxDataString &mk2, const JagStrSplit &sp2 )
{
	if ( mk1 == JAG_CJAG && mk2 == JAG_CJAG ) return false;
	if ( mk1 == JAG_CJAG && mk2 == JAG_OJAG ) return false;

	int start1 = 0;
	int start2 = 0;
	double bbx1, bby1, brx1, bry1;
	double bbx2, bby2, brx2, bry2;
	double xmin, ymin, xmax, ymax;
   	const char *str;
   	double dx, dy;
	char *p, *pdata;

	xmin = ymin = LONG_MAX; xmax = ymax = LONG_MIN;

	int rc;
	if ( mk1 == JAG_OJAG ) {
		start1 = 1;
	} 
	getPolygonBound( mk1, sp1, bbx1, bby1, brx1, bry1 );

	if ( mk2 == JAG_OJAG ) {
		boundingBoxRegion(sp2[0], bbx2, bby2, brx2, bry2 );
		start2 = 1;
	} else {
		pdata = secondTokenStart( sp2.c_str() );
		if ( ! pdata ) return false;
	}
	getPolygonBound( mk2, sp2, bbx2, bby2, brx2, bry2 );

	if ( bound2DDisjoint( bbx1, bby1, brx1, bry1,  bbx2, bby2, brx2, bry2 ) ) {
		prt(("s7581 bound2DDisjoint two polygon not within\n" ));
		return false;
	}

	if (   mk1 == JAG_OJAG  &&  mk2 == JAG_CJAG ) {
    	JagPolygon pgon;  // each polygon can have multiple linestrings
    	rc = JagParser::addPolygonData( pgon, pdata, true, false );
    	//prt(("s3388 addPolygonData pgon: print():\n" ));
    	//pgon.print();
    
    	if ( rc <  0 ) { return false; }
    	if ( pgon.size() < 1 ) { return false; }
    
    	// sp1 array:  "bbox  x:y x:y  ... | x:y  x:y ...| ..." sp1: start=1 skip '|' and '!'
    	// sp2 cstr:  ( ( x y, x y, ...), ( ... ), (...) )
    	// pgon has sp2 data parsed
    	// check first polygon only for now
		bool skip = false;
    	for ( int i=start1; i < sp1.length(); ++i ) {
			if ( sp1[i] == "|" ) {  skip = true; }
			else if ( sp1[i] == "!" ) {  skip = false; }
			else {
				if ( skip ) continue;
    			str = sp1[i].c_str();
    			if ( strchrnum( str, ':') < 1 ) continue;
    			get2double(str, p, ':', dx, dy );
    			if ( ! pointWithinPolygon( dx, dy, pgon.linestr[0] ) ) { return false; }
			}
    	}
    	return true;
	} else if (  mk1 == JAG_OJAG  &&  mk2 == JAG_OJAG ) {
		// get a linestring from cords in sp2
		JagLineString3D linestr;
		bool skip = false;
		for ( int i=start2; i < sp2.length(); ++i ) {
			if ( sp2[i] == "|" ) {  skip = true; }
			else if ( sp2[i] == "!" ) {  skip = false; }
			else {
				if ( skip ) continue;
    			str = sp2[i].c_str();
    			if ( strchrnum( str, ':') < 1 ) continue;
    			get2double(str, p, ':', dx, dy );
				JagPoint2D pt(dx,dy);
				linestr.add(pt);
			}
		}

		skip = false;
    	for ( int i=start1; i < sp1.length(); ++i ) {
    		//if ( sp1[i] == "|" || sp1[i] == "!" ) break;
			if ( sp1[i] == "|" ) {  skip = true; }
			else if ( sp1[i] == "!" ) {  skip = false; }
			else {
				if ( skip ) continue;
    			str = sp1[i].c_str();
    			if ( strchrnum( str, ':') < 1 ) continue;
    			get2double(str, p, ':', dx, dy );
    			if ( ! pointWithinPolygon( dx, dy, linestr ) ) { return false; }
			}
    	}
    	return true;
	} else {
    	return false;
	}
}



//////////////////////////// 2D linestring intersect  //////////////////////////////////////////////////
bool JagGeo::lineStringIntersectLineString( const AbaxDataString &mk1, const JagStrSplit &sp1,
			                                const AbaxDataString &mk2, const JagStrSplit &sp2 )
{
	// sweepline algo
	int start1 = 0;
	if ( mk1 == JAG_OJAG ) { start1 = 1; }
	int start2 = 0;
	if ( mk2 == JAG_OJAG ) { start2 = 1; }


	double dx1, dy1, dx2, dy2, t;
	const char *str;
	char *p; int i;
	int totlen = sp1.length()-start1 + sp2.length() - start2;
	JagSortPoint2D *points = new JagSortPoint2D[2*totlen];
	int j = 0;
	int rc;
	for ( i=start1; i < sp1.length()-1; ++i ) {
		str = sp1[i].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx1, dy1 );

		str = sp1[i+1].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx2, dy2 );

		if ( jagEQ(dx1, dx2)) {
			dx2 += 0.000001f;
		}

		if ( dx1 > dx2 ) {
			// swap
			t = dx1; dx1 = dx2; dx2 = t; 
			t = dy1; dy1 = dy2; dy2 = t; 
		}

		points[j].x1 = dx1; points[j].y1 = dy1;
		points[j].x2 = dx2; points[j].y2 = dy2;
		points[j].end = JAG_LEFT;
		points[j].color = JAG_RED;
		++j;

		points[j].x1 = dx1; points[j].y1 = dy1;
		points[j].x2 = dx2; points[j].y2 = dy2;
		points[j].end = JAG_RIGHT;
		points[j].color = JAG_RED;
		++j;
	}

	for ( i=start2; i < sp2.length()-1; ++i ) {
		str = sp2[i].c_str();

		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx1, dy1 );

		str = sp2[i+1].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx2, dy2 );

		if ( jagEQ(dx1, dx2) )  {
			dx2 += 0.000001f;
		}

		if ( dx1 > dx2 ) {
			// swap
			t = dx1; dx1 = dx2; dx2 = t; 
			t = dy1; dy1 = dy2; dy2 = t; 
		}

		points[j].x1 = dx1; points[j].y1 = dy1;
		points[j].x2 = dx2; points[j].y2 = dy2;
		points[j].end = JAG_LEFT;
		points[j].color = JAG_BLUE;
		++j;

		points[j].x1 = dx1; points[j].y1 = dy1;
		points[j].x2 = dx2; points[j].y2 = dy2;
		points[j].end = JAG_RIGHT;
		points[j].color = JAG_BLUE;
		++j;

	}

	int len = j;
	rc = sortLinePoints( points, len );
	if ( rc ) {
		//prt(("s7732 sortIntersectLinePoints rc=%d retur true intersect\n", rc ));
		// return true;
	}

	JagArray<JagLineSeg2DPair> *jarr = new JagArray<JagLineSeg2DPair>();
	const JagLineSeg2DPair *above;
	const JagLineSeg2DPair *below;
	JagLineSeg2DPair seg; seg.value = '1';
	for ( int i=0; i < len; ++i ) {
		seg.key.x1 =  points[i].x1; seg.key.y1 =  points[i].y1;
		seg.key.x2 =  points[i].x2; seg.key.y2 =  points[i].y2;
		seg.color = points[i].color;
		/**
		prt(("s0088 seg print:\n" ));
		seg.println();
		**/

		if ( JAG_LEFT == points[i].end ) {
			jarr->insert( seg );
			above = jarr->getSucc( seg );
			below = jarr->getPred( seg );

			#if 0
			prt(("s1290 JAG_LEFT: \n" ));
			if ( above ) {
				prt(("s7781 above print: \n" ));
				above->println();
			}
			if ( below ) {
				prt(("s7681 below print: \n" ));
				below->println();
			}

			if ( above && *above == JagLineSeg2DPair::NULLVALUE ) {
				prt(("s6201 above is NULLVALUE abort\n" ));
				//abort();
				above = NULL;
			}

			if ( below && *below == JagLineSeg2DPair::NULLVALUE ) {
				prt(("s2098 below is NULLVALUE abort\n" ));
				//abort();
				below = NULL;
			}
			#endif

			if ( above && below ) {
				if ( above->color != below->color 
				     && lineIntersectLine( above->key.x1,above->key.y1,above->key.x2,above->key.y2, 
									    below->key.x1,below->key.y1,below->key.x2,below->key.y2) ) {
					//prt(("s7640 left above below intersect\n" ));
					return true;
				}
			} else if ( above ) {
				if ( above->color != seg.color 
					 && lineIntersectLine( above->key.x1,above->key.y1,above->key.x2,above->key.y2, 
									    seg.key.x1,seg.key.y1,seg.key.x2,seg.key.y2) ) {
					/***
					prt(("s7641 left above seg intersect\n" ));
					above->println();
					seg.println();
					***/
					return true;
				}
			} else if ( below ) {
				if ( below->color != seg.color 
					 && lineIntersectLine( below->key.x1,below->key.y1,below->key.x2,below->key.y2, 
									    seg.key.x1,seg.key.y1,seg.key.x2,seg.key.y2) ) {
					//prt(("s7641 left below seg intersect\n" ));
					return true;
				}
			}
		} else {
			// right end
			above = jarr->getSucc( seg );
			below = jarr->getPred( seg );

			#if 0
			prt(("s1290 JAG_RIGHT: \n" ));
			if ( above ) {
				prt(("s7781 above print: \n" ));
				above->println();
			}
			if ( below ) {
				prt(("s7681 below print: \n" ));
				below->println();
			}

			if ( above && *above == JagLineSeg2DPair::NULLVALUE ) {
				prt(("s7201 above is NULLVALUE abort\n" ));
				//abort();
				above = NULL;
			}

			if ( below && *below == JagLineSeg2DPair::NULLVALUE ) {
				/***
				prt(("72098 below is NULLVALUE abort\n" ));
				jarr->printKey();
				seg.println();
				//abort();
				***/
				below = NULL;
			}
			#endif

			if ( above && below ) {
				if ( above->color != below->color 
					 && lineIntersectLine( above->key.x1,above->key.y1,above->key.x2,above->key.y2, 
									    below->key.x1,below->key.y1,below->key.x2,below->key.y2) ) {
					//prt(("s7740 rightend above below intersect\n" ));
					return true;
				}
			} else if ( above ) {
				if ( above->color != seg.color 
					 && lineIntersectLine( above->key.x1,above->key.y1,above->key.x2,above->key.y2, 
									    seg.key.x1,seg.key.y1,seg.key.x2,seg.key.y2) ) {
					//prt(("s7740 rightend above seg intersect\n" ));
					return true;
				}
			} else if ( below ) {
				if ( below->color != seg.color 
					 && lineIntersectLine( below->key.x1,below->key.y1,below->key.x2,below->key.y2, 
									    seg.key.x1,seg.key.y1,seg.key.x2,seg.key.y2) ) {
					//prt(("s7740 rightend below seg intersect\n" ));
					return true;
				}
			}
			jarr->remove( seg );
		}
	}

	delete [] points;
	delete jarr;

	prt(("s7740 no intersect\n"));
	return false;
}


bool JagGeo::lineStringIntersectTriangle( const AbaxDataString &mk1, const JagStrSplit &sp1,
			                         double x1, double y1, double x2, double y2, double x3, double y3, bool strict )

{
	double trix, triy, rx, ry;
	triangleRegion(x1,y1, x2,y2, x3,y3, trix, triy, rx, ry );
	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  trix, triy, rx, ry ) ) {
			return false;
		}
		start = 1;
	}

	double dx1, dy1, dx2, dy2;
	const char *str;
	char *p; int i;
	for ( i=start; i < sp1.length()-1; ++i ) {
    	// line: x10, y10 --> x20, y20 
		str = sp1[i].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx1, dy1 );
		if ( pointInTriangle( dx1, dy1, x1,y1, x2,y2, x3,y3, strict, false) ) return true;

		str = sp1[i+1].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx2, dy2 );

		if ( lineIntersectLine(dx1,dy1,dx2,dy2, x1,y1,x2,y2 ) ) return true;
		if ( lineIntersectLine(dx1,dy1,dx2,dy2, x2,y2,x3,y3 ) ) return true;
		if ( lineIntersectLine(dx1,dy1,dx2,dy2, x3,y3,x1,y1 ) ) return true;
	}

	str = sp1[i].c_str();
	if ( strchrnum( str, ':') >= 1 ) {
		get2double(str, p, ':', dx1, dy1 );
		if ( pointInTriangle( dx1, dy1, x1,y1, x2,y2, x3,y3, strict, false) ) return true;
	}

	return false;
}

bool JagGeo::lineStringIntersectRectangle( const AbaxDataString &mk1, const JagStrSplit &sp1,
                                         double x0, double y0, double a, double b, double nx, bool strict )
{
	if ( ! validDirection(nx) ) return false;
	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  x0, y0, a, b ) ) {
			return false;
		}
		start = 1;
	}

	JagLine2D line[4];
	edgesOfRectangle( a, b, line );
	double gx1,gy1, gx2,gy2;
	for ( int i=0; i < 4; ++i ) {
		line[i].transform(x0,y0,nx);
	}
	
    double dx1, dy1, dx2, dy2;
    const char *str;
    char *p; int i;
	for ( i=start; i < sp1.length()-1; ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 1 ) continue;
        get2double(str, p, ':', dx1, dy1 );
		if ( pointWithinRectangle( dx1, dy1, x0,y0,a,b,nx, strict ) ) return true;
        str = sp1[i+1].c_str();
        if ( strchrnum( str, ':') < 1 ) continue;
        get2double(str, p, ':', dx2, dy2 );

		for ( int j=0;j<4;++j) {
			if ( lineIntersectLine( dx1,dy1,dx2,dy2, line[j].x1,line[j].y1,line[j].x2,line[j].y2 ) ) return true;
		}
	}

    str = sp1[i].c_str();
    if ( strchrnum( str, ':') >= 1 ) {
    	get2double(str, p, ':', dx1, dy1 );
		if ( pointWithinRectangle( dx1, dy1, x0,y0,a,b,nx, strict ) ) return true;
	}

	return false;
}

bool JagGeo::lineStringIntersectEllipse( const AbaxDataString &mk1, const JagStrSplit &sp1,
									double x0, double y0, double a, double b, double nx, bool strict )
{
	if ( ! validDirection(nx) ) return false;
	if ( jagIsZero(a) || jagIsZero(b) ) return false;

	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  x0, y0, a, b ) ) {
			return false;
		}
		start = 1;
	}

    double dx1, dy1, dx2, dy2;
    const char *str;
    char *p; int i;
	for ( i=start; i < sp1.length()-1; ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 1 ) continue;
        get2double(str, p, ':', dx1, dy1 );
		if ( pointWithinEllipse( dx1, dy1, x0,y0,a,b,nx, strict ) ) return true;
        str = sp1[i+1].c_str();
        if ( strchrnum( str, ':') < 1 ) continue;
        get2double(str, p, ':', dx2, dy2 );
		if ( lineIntersectEllipse( dx1,dy1,dx2,dy2, x0, y0, a, b, nx, strict ) ) return true;
	}

    str = sp1[i].c_str();
    if ( strchrnum( str, ':') >= 1 ) {
    	get2double(str, p, ':', dx1, dy1 );
		if ( pointWithinEllipse( dx1, dy1, x0,y0,a,b,nx, strict ) ) return true;
	}

	return false;
}


//////////////////////////// 2D polygon intersect  //////////////////////////////////////////////////
bool JagGeo::polygonIntersectLineString( const AbaxDataString &mk1, const JagStrSplit &sp1,
			                                const AbaxDataString &mk2, const JagStrSplit &sp2 )
{
	// sweepline algo
	int start1 = 0;
	if ( mk1 == JAG_OJAG ) { start1 = 1; }
	int start2 = 0;
	if ( mk2 == JAG_OJAG ) { start2 = 1; }

	//prt(("s8019 polygonIntersectLineString sp1: sp2:\n" ));
	//sp1.print();
	//sp2.print();


	double dx1, dy1, dx2, dy2, t;
	const char *str;
	char *p; int i;
	int totlen = sp1.length()-start1 + sp2.length() - start2;
	JagSortPoint2D *points = new JagSortPoint2D[2*totlen];
	int j = 0;
	int rc;
	for ( i=start1; i < sp1.length()-1; ++i ) {
		str = sp1[i].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;  // skip |
		get2double(str, p, ':', dx1, dy1 );

		str = sp1[i+1].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;  // skip |
		get2double(str, p, ':', dx2, dy2 );

		if ( jagEQ(dx1, dx2)) {
			dx2 += 0.000001f;
		}

		if ( dx1 > dx2 ) {
			// swap
			t = dx1; dx1 = dx2; dx2 = t; 
			t = dy1; dy1 = dy2; dy2 = t; 
		}

		points[j].x1 = dx1; points[j].y1 = dy1;
		points[j].x2 = dx2; points[j].y2 = dy2;
		points[j].end = JAG_LEFT;
		points[j].color = JAG_RED;
		++j;

		points[j].x1 = dx1; points[j].y1 = dy1;
		points[j].x2 = dx2; points[j].y2 = dy2;
		points[j].end = JAG_RIGHT;
		points[j].color = JAG_RED;
		++j;
	}

	for ( i=start2; i < sp2.length()-1; ++i ) {
		str = sp2[i].c_str();

		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx1, dy1 );

		str = sp2[i+1].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx2, dy2 );

		if ( jagEQ(dx1, dx2) )  {
			dx2 += 0.000001f;
		}

		if ( dx1 > dx2 ) {
			// swap
			t = dx1; dx1 = dx2; dx2 = t; 
			t = dy1; dy1 = dy2; dy2 = t; 
		}

		points[j].x1 = dx1; points[j].y1 = dy1;
		points[j].x2 = dx2; points[j].y2 = dy2;
		points[j].end = JAG_LEFT;
		points[j].color = JAG_BLUE;
		++j;

		points[j].x1 = dx1; points[j].y1 = dy1;
		points[j].x2 = dx2; points[j].y2 = dy2;
		points[j].end = JAG_RIGHT;
		points[j].color = JAG_BLUE;
		++j;

	}

	int len = j;
	rc = sortLinePoints( points, len );
	if ( rc ) {
		//prt(("s7732 sortIntersectLinePoints rc=%d retur true intersect\n", rc ));
		// return true;
	}

	JagArray<JagLineSeg2DPair> *jarr = new JagArray<JagLineSeg2DPair>();
	const JagLineSeg2DPair *above;
	const JagLineSeg2DPair *below;
	JagLineSeg2DPair seg; seg.value = '1';
	for ( int i=0; i < len; ++i ) {
		seg.key.x1 =  points[i].x1; seg.key.y1 =  points[i].y1;
		seg.key.x2 =  points[i].x2; seg.key.y2 =  points[i].y2;
		seg.color = points[i].color;
		/**
		prt(("s0088 seg print:\n" ));
		seg.println();
		**/

		if ( JAG_LEFT == points[i].end ) {
			jarr->insert( seg );
			above = jarr->getSucc( seg );
			below = jarr->getPred( seg );

			#if 0
			prt(("s1290 JAG_LEFT: \n" ));
			if ( above ) {
				prt(("s7781 above print: \n" ));
				above->println();
			}
			if ( below ) {
				prt(("s7681 below print: \n" ));
				below->println();
			}

			if ( above && *above == JagLineSeg2DPair::NULLVALUE ) {
				prt(("s6201 above is NULLVALUE abort\n" ));
				//abort();
				above = NULL;
			}

			if ( below && *below == JagLineSeg2DPair::NULLVALUE ) {
				prt(("s2098 below is NULLVALUE abort\n" ));
				//abort();
				below = NULL;
			}
			#endif

			if ( above && below ) {
				if ( above->color != below->color 
				     && lineIntersectLine( above->key.x1,above->key.y1,above->key.x2,above->key.y2, 
									    below->key.x1,below->key.y1,below->key.x2,below->key.y2) ) {
					//prt(("s7640 left above below intersect\n" ));
					return true;
				}
			} else if ( above ) {
				if ( above->color != seg.color 
					 && lineIntersectLine( above->key.x1,above->key.y1,above->key.x2,above->key.y2, 
									    seg.key.x1,seg.key.y1,seg.key.x2,seg.key.y2) ) {
					/***
					prt(("s7641 left above seg intersect\n" ));
					above->println();
					seg.println();
					***/
					return true;
				}
			} else if ( below ) {
				if ( below->color != seg.color 
					 && lineIntersectLine( below->key.x1,below->key.y1,below->key.x2,below->key.y2, 
									    seg.key.x1,seg.key.y1,seg.key.x2,seg.key.y2) ) {
					//prt(("s7641 left below seg intersect\n" ));
					return true;
				}
			}
		} else {
			// right end
			above = jarr->getSucc( seg );
			below = jarr->getPred( seg );

			#if 0
			prt(("s1290 JAG_RIGHT: \n" ));
			if ( above ) {
				prt(("s7781 above print: \n" ));
				above->println();
			}
			if ( below ) {
				prt(("s7681 below print: \n" ));
				below->println();
			}

			if ( above && *above == JagLineSeg2DPair::NULLVALUE ) {
				prt(("s7201 above is NULLVALUE abort\n" ));
				//abort();
				above = NULL;
			}

			if ( below && *below == JagLineSeg2DPair::NULLVALUE ) {
				/***
				prt(("72098 below is NULLVALUE abort\n" ));
				jarr->printKey();
				seg.println();
				//abort();
				***/
				below = NULL;
			}
			#endif

			if ( above && below ) {
				if ( above->color != below->color 
					 && lineIntersectLine( above->key.x1,above->key.y1,above->key.x2,above->key.y2, 
									    below->key.x1,below->key.y1,below->key.x2,below->key.y2) ) {
					//prt(("s7740 rightend above below intersect\n" ));
					return true;
				}
			} else if ( above ) {
				if ( above->color != seg.color 
					 && lineIntersectLine( above->key.x1,above->key.y1,above->key.x2,above->key.y2, 
									    seg.key.x1,seg.key.y1,seg.key.x2,seg.key.y2) ) {
					//prt(("s7740 rightend above seg intersect\n" ));
					return true;
				}
			} else if ( below ) {
				if ( below->color != seg.color 
					 && lineIntersectLine( below->key.x1,below->key.y1,below->key.x2,below->key.y2, 
									    seg.key.x1,seg.key.y1,seg.key.x2,seg.key.y2) ) {
					//prt(("s7740 rightend below seg intersect\n" ));
					return true;
				}
			}
			jarr->remove( seg );
		}
	}

	delete [] points;
	delete jarr;

	//prt(("s7740 no intersect\n"));
	return false;
}

bool JagGeo::polygonIntersectTriangle( const AbaxDataString &mk1, const JagStrSplit &sp1,
			                         double x1, double y1, double x2, double y2, double x3, double y3, bool strict )
{
	double trix, triy, rx, ry;
	triangleRegion(x1,y1, x2,y2, x3,y3, trix, triy, rx, ry );
	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  trix, triy, rx, ry ) ) {
			return false;
		}
		start = 1;
	}

	double dx1, dy1, dx2, dy2;
	const char *str;
	char *p; int i;
	for ( i=start; i < sp1.length()-2; ++i ) {
    	// line: x10, y10 --> x20, y20 
		str = sp1[i].c_str();
		if ( *str == '|' || *str == '!' ) break;  // test outer polygon only
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx1, dy1 );
		if ( pointInTriangle( dx1, dy1, x1,y1, x2,y2, x3,y3, strict, false) ) return true;

		str = sp1[i+1].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx2, dy2 );

		if ( lineIntersectLine(dx1,dy1,dx2,dy2, x1,y1,x2,y2 ) ) return true;
		if ( lineIntersectLine(dx1,dy1,dx2,dy2, x2,y2,x3,y3 ) ) return true;
		if ( lineIntersectLine(dx1,dy1,dx2,dy2, x3,y3,x1,y1 ) ) return true;
	}

	str = sp1[i].c_str();
	if ( strchrnum( str, ':') >= 1 ) {
		get2double(str, p, ':', dx1, dy1 );
		if ( pointInTriangle( dx1, dy1, x1,y1, x2,y2, x3,y3, strict, false) ) return true;
	}

	return false;
}


bool JagGeo::polygonIntersectLine( const AbaxDataString &mk1, const JagStrSplit &sp1, 
								   double x1, double y1, double x2, double y2 )
{
	double trix, triy, rx, ry;
	trix = (x1+x2)/2.0;
	triy = (y1+y2)/2.0;
	rx = fabs( trix - jagmin(x1,x2) );
	ry = fabs( triy - jagmin(y1,y2) );
	//prt(("s1209 x1=%.2f y1=%.2f  x2=%.2f y2=%.2f\n", x1, y1, x2, y2 ));
	//prt(("s2288 line x1 trix=%.2f triy=%.2f rx=%.2f ry=%.2f\n", trix, triy, rx, ry ));
	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		//prt(("s3062 sp1[]0]=[%s] bbx=%.2f bby=%.2f brx=%.2f bry=%.2f\n", sp1[0].c_str(), bbx, bby, brx, bry ));
		if ( bound2DDisjoint( bbx, bby, brx, bry,  trix, triy, rx, ry ) ) {
			//prt(("s3383 bound2DDisjoint sp1[0]=[%s]\n", sp1[0].c_str() ));
			return false;
		}
		start = 1;
	}

	JagPolygon pgon;
	int rc = JagParser::addPolygonData( pgon, sp1, false );
	if ( rc < 0 ) {
		//prt(("s3647 addPolygonData rc=%d < 0  false\n", rc ));
		return false;
	}

	if ( pointWithinPolygon( x1, y1, pgon ) ) { 
		//prt(("s3283 x1, y1 in pgon\n" ));
		return true; 
	}

	if ( pointWithinPolygon( x2, y2, pgon ) ) { 
		//prt(("s3283 x2, y2 in pgon\n" ));
		return true; 
	}

	double dx1, dy1, dx2, dy2;
	const char *str;
	char *p; int i;
	for ( i=start; i < sp1.length()-2; ++i ) {
    	// line: x10, y10 --> x20, y20 
		str = sp1[i].c_str();
		if ( *str == '|' || *str == '!' ) break;  // test outer polygon only
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx1, dy1 );

		str = sp1[i+1].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx2, dy2 );

		if ( lineIntersectLine(dx1,dy1,dx2,dy2, x1,y1,x2,y2 ) ) {
			//prt(("s9737 lineIntersectLine true\n" ));
			return true;
		}
	}

	return false;
}

bool JagGeo::polygonIntersectRectangle( const AbaxDataString &mk1, const JagStrSplit &sp1,
                                         double x0, double y0, double a, double b, double nx, bool strict )
{
	if ( ! validDirection(nx) ) return false;
	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  x0, y0, a, b ) ) {
			return false;
		}
		start = 1;
	}

	//prt(("s8612 polygonIntersectRectangle sp1:\n" ));
	//sp1.print();

	JagLine2D line[4];
	edgesOfRectangle( a, b, line );
	double gx1,gy1, gx2,gy2;
	for ( int i=0; i < 4; ++i ) {
		line[i].transform(x0,y0,nx);
	}
	
    double dx1, dy1, dx2, dy2;
    const char *str;
    char *p; int i;
	for ( i=start; i < sp1.length()-2; ++i ) {
        str = sp1[i].c_str();
		if ( *str == '|' || *str == '!' ) break;
        if ( strchrnum( str, ':') < 1 ) continue;
        get2double(str, p, ':', dx1, dy1 );
		if ( pointWithinRectangle( dx1, dy1, x0,y0,a,b,nx, strict ) ) return true;
        str = sp1[i+1].c_str();
        if ( strchrnum( str, ':') < 1 ) continue;
        get2double(str, p, ':', dx2, dy2 );

		for ( int j=0;j<4;++j) {
			if ( lineIntersectLine( dx1,dy1,dx2,dy2, line[j].x1,line[j].y1,line[j].x2,line[j].y2 ) ) return true;
		}
	}

    str = sp1[i].c_str();
    if ( strchrnum( str, ':') >= 1 ) {
    	get2double(str, p, ':', dx1, dy1 );
		if ( pointWithinRectangle( dx1, dy1, x0,y0,a,b,nx, strict ) ) return true;
	}

	return false;
}

bool JagGeo::polygonIntersectEllipse( const AbaxDataString &mk1, const JagStrSplit &sp1,
									double x0, double y0, double a, double b, double nx, bool strict )
{
	if ( ! validDirection(nx) ) return false;
	if ( jagIsZero(a) || jagIsZero(b) ) return false;

	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		double bbx, bby, brx, bry;
		boundingBoxRegion(sp1[0], bbx, bby, brx, bry );
		if ( bound2DDisjoint( bbx, bby, brx, bry,  x0, y0, a, b ) ) {
			return false;
		}
		start = 1;
	}

    double dx1, dy1, dx2, dy2;
    const char *str;
    char *p; int i;
	for ( i=start; i < sp1.length()-2; ++i ) {
        str = sp1[i].c_str();
		if ( *str == '|' || *str == '!' ) break;
        if ( strchrnum( str, ':') < 1 ) continue;
        get2double(str, p, ':', dx1, dy1 );
		if ( pointWithinEllipse( dx1, dy1, x0,y0,a,b,nx, strict ) ) return true;
        str = sp1[i+1].c_str();
        if ( strchrnum( str, ':') < 1 ) continue;
        get2double(str, p, ':', dx2, dy2 );
		if ( lineIntersectEllipse( dx1,dy1,dx2,dy2, x0, y0, a, b, nx, strict ) ) return true;
	}

    str = sp1[i].c_str();
    if ( strchrnum( str, ':') >= 1 ) {
    	get2double(str, p, ':', dx1, dy1 );
		if ( pointWithinEllipse( dx1, dy1, x0,y0,a,b,nx, strict ) ) return true;
	}

	return false;
}


//////////////////////////// 2D triangle //////////////////////////////////////////////////
bool JagGeo::triangleWithinTriangle( double x10, double y10, double x20, double y20, double x30, double y30,
			                         double x1, double y1, double x2, double y2, double x3, double y3, bool strict )

{
   	JagPoint2D p10( x10, y10 );
   	JagPoint2D p20( x20, y20 );
   	JagPoint2D p30( x30, y30 );

   	JagPoint2D p1( x1, y1 );
   	JagPoint2D p2( x2, y2 );
   	JagPoint2D p3( x3, y3 );

	bool rc = pointWithinTriangle( p10, p1, p2, p3, strict, true );
	if ( ! rc ) return false;

	rc = pointWithinTriangle( p20, p1, p2, p3, strict, true );
	if ( ! rc ) return false;

	rc = pointWithinTriangle( p30, p1, p2, p3, strict, true );
	if ( ! rc ) return false;

	return true;
}

/***
bool JagGeo::triangleWithinSquare( double x10, double y10, double x20, double y20, double x30, double y30,
                           double x0, double y0, double r, double nx, bool strict )
{
	if ( ! validDirection(nx) ) return false;
	if ( bound2DTriangleDisjoint( x10, y10, x20, y20, x30, y30, x0, y0, r,r ) ) return false;

	double sq_x[3], sq_y[3];
	sq_x[0] = x10; sq_y[0] = y10;
	sq_x[1] = x20; sq_y[1] = y20;
	sq_x[2] = x30; sq_y[2] = y30;
	double loc_x, loc_y;
	for ( int i=0; i < 3; ++i ) {
    	transform2DCoordGlobal2Local( x0, y0, sq_x[i], sq_y[i], nx, loc_x, loc_y );
		if ( ! locIn2DCenterBox( loc_x, loc_y, r,r, strict ) ) { return false; }
	}
	return true;
}
***/

bool JagGeo::triangleWithinRectangle( double x10, double y10, double x20, double y20, double x30, double y30,
                                         double x0, double y0, double a, double b, double nx, bool strict )
{
	if ( ! validDirection(nx) ) return false;
	if ( bound2DTriangleDisjoint( x10, y10, x20, y20, x30, y30, x0, y0, a,b ) ) return false;
	double sq_x[3], sq_y[3];
	sq_x[0] = x10; sq_y[0] = y10;
	sq_x[1] = x20; sq_y[1] = y20;
	sq_x[2] = x30; sq_y[2] = y30;
	double loc_x, loc_y;
	for ( int i=0; i < 3; ++i ) {
    	transform2DCoordGlobal2Local( x0, y0, sq_x[i], sq_y[i], nx, loc_x, loc_y );
		if ( ! locIn2DCenterBox( loc_x, loc_y, a,b, strict ) ) { return false; }
	}
	return true;
}

bool JagGeo::triangleWithinCircle( double x10, double y10, double x20, double y20, double x30, double y30,
								   double x0, double y0, double r, double nx, bool strict )

{
	if ( ! validDirection(nx) ) return false;
	if ( bound2DTriangleDisjoint( x10, y10, x20, y20, x30, y30, x0, y0, r,r ) ) return false;
	double sq_x[3], sq_y[3];
	sq_x[0] = x10; sq_y[0] = y10;
	sq_x[1] = x20; sq_y[1] = y20;
	sq_x[2] = x30; sq_y[2] = y30;
	double loc_x, loc_y;
	for ( int i=0; i < 3; ++i ) {
    	transform2DCoordGlobal2Local( x0, y0, sq_x[i], sq_y[i], nx, loc_x, loc_y );
		if ( ! pointWithinCircle( loc_x, loc_y, 0.0, 0.0, r, strict ) ) { return false; }

	}
	return true;
}


bool JagGeo::triangleWithinEllipse( double x10, double y10, double x20, double y20, double x30, double y30,
									double x0, double y0, double a, double b, double nx, bool strict )

{
	if ( ! validDirection(nx) ) return false;
	if ( jagIsZero(a) || jagIsZero(b) ) return false;
	if ( bound2DTriangleDisjoint( x10, y10, x20, y20, x30, y30, x0, y0, a,b ) ) return false;

	double tri_x[3], tri_y[3];
	tri_x[0] = x10; tri_y[0] = y10;
	tri_x[1] = x20; tri_y[1] = y20;
	tri_x[2] = x30; tri_y[2] = y30;
	double loc_x, loc_y;
	for ( int i=0; i < 3; ++i ) {
    	transform2DCoordGlobal2Local( x0, y0, tri_x[i], tri_y[i], nx, loc_x, loc_y );
		if ( ! pointWithinNormalEllipse( loc_x, loc_y, a, b, strict ) ) { return false; }
	}
	return true;
}

bool JagGeo::triangleWithinPolygon( double x10, double y10, double x20, double y20, double x30, double y30,
									const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict )

{
	double bbx, bby, rx, ry;
	getPolygonBound( mk2, sp2, bbx, bby, rx, ry );
	if ( bound2DTriangleDisjoint( x10, y10, x20, y20, x30, y30, bbx, bby, rx, ry ) ) {
		return false;
	}

	JagPolygon pgon;
	int rc = JagParser::addPolygonData( pgon, sp2, false );
	if ( rc < 0 ) return false;

	double tri_x[3], tri_y[3];
	tri_x[0] = x10; tri_y[0] = y10;
	tri_x[1] = x20; tri_y[1] = y20;
	tri_x[2] = x30; tri_y[2] = y30;
	for ( int i=0; i < 3; ++i ) {
		if ( ! pointWithinPolygon( tri_x[i], tri_y[i], pgon ) ) {
			return false;
		}
	}
	return true;
}


///////////////////////// 2D ellipse
bool JagGeo::ellipseWithinTriangle( double px0, double py0, double a0, double b0, double nx0,
									double x1, double y1, double x2, double y2, double x3, double y3,
									bool strict )
{
	if ( ! validDirection(nx0) ) return false;
	if ( bound2DTriangleDisjoint( x1, y1, x2, y2, x3, y3, px0, py0, a0,b0 ) ) return false;

   	JagPoint2D p1( x1, y1 );
   	JagPoint2D p2( x2, y2 );
   	JagPoint2D p3( x3, y3 );
	JagVector<JagPoint2D> vec;
	JagPoint2D psq;
	bool rc;
	samplesOn2DEllipse( px0, py0, a0, b0, nx0, NUM_SAMPLE, vec );
	for ( int i=0; i <vec.size(); ++i ) {
   		psq.x = vec[i].x;
   		psq.y = vec[i].y;
		rc = pointWithinTriangle( psq, p1, p2, p3, strict, true );
		if ( ! rc ) return false;
	}
	return true;
}

bool JagGeo::ellipseWithinSquare( double px0, double py0, double a0, double b0, double nx0,
							 	double x0, double y0, double r, double nx, bool strict )

{
	if ( ! validDirection(nx0) ) return false;
	if ( ! validDirection(nx) ) return false;
	if (  bound2DDisjoint( px0, py0, a0,b0, x0, y0, r,r ) ) { return false; }

	JagVector<JagPoint2D> vec;
	double loc_x, loc_y;
	samplesOn2DEllipse( px0, py0, a0, b0, nx0, NUM_SAMPLE, vec );
	for ( int i=0; i <vec.size(); ++i ) {
    	transform2DCoordGlobal2Local( x0, y0, vec[i].x, vec[i].y, nx, loc_x, loc_y );
		if ( ! locIn2DCenterBox( loc_x, loc_y, r, r, strict ) ) { return false; }
	}
	return true;
}

bool JagGeo::ellipseWithinRectangle( double px0, double py0, double a0, double b0, double nx0,
                                	double x0, double y0, double a, double b, double nx, bool strict )
{
	if ( ! validDirection(nx0) ) return false;
	if ( ! validDirection(nx) ) return false;
	if (  bound2DDisjoint( px0, py0, a0,b0, x0, y0, a,b ) ) { return false; }
	JagVector<JagPoint2D> vec;
	double loc_x, loc_y;
	samplesOn2DEllipse( px0, py0, a0, b0, nx0, NUM_SAMPLE, vec );
	for ( int i=0; i <vec.size(); ++i ) {
    	transform2DCoordGlobal2Local( x0, y0, vec[i].x, vec[i].y, nx, loc_x, loc_y );
		if ( ! locIn2DCenterBox( loc_x, loc_y, a, b, strict ) ) { return false; }
	}
	return true;

}

bool JagGeo::ellipseWithinCircle( double px0, double py0, double a0, double b0, double nx0,
                                	double x0, double y0, double r, double nx, bool strict )
{
	if ( ! validDirection(nx0) ) return false;
	if ( ! validDirection(nx) ) return false;
	if (  bound2DDisjoint( px0, py0, a0,b0, x0, y0, r,r ) ) { return false; }
	JagVector<JagPoint2D> vec;
	double loc_x, loc_y;
	samplesOn2DEllipse( px0, py0, a0, b0, nx0, NUM_SAMPLE, vec );
	for ( int i=0; i <vec.size(); ++i ) {
    	transform2DCoordGlobal2Local( x0, y0, vec[i].x, vec[i].y, nx, loc_x, loc_y );
		if ( ! pointWithinCircle( loc_x, loc_y, 0.0, 0.0, r, strict ) ) { return false; }
	}
	return true;
}


bool JagGeo::ellipseWithinEllipse( double px0, double py0, double a0, double b0, double nx0,
                                	double x0, double y0, double a, double b, double nx, bool strict )
{
	if ( ! validDirection(nx0) ) return false;
	if ( ! validDirection(nx) ) return false;
	if (  bound2DDisjoint( px0, py0, a0,b0, x0, y0, a,a ) ) { return false; }
	JagVector<JagPoint2D> vec;
	double loc_x, loc_y;
	samplesOn2DEllipse( px0, py0, a0, b0, nx0, NUM_SAMPLE, vec );
	for ( int i=0; i <vec.size(); ++i ) {
    	transform2DCoordGlobal2Local( x0, y0, vec[i].x, vec[i].y, nx, loc_x, loc_y );
		if ( ! pointWithinNormalEllipse( loc_x, loc_y, a, b, strict ) ) { return false; }
	}
	return true;
}

bool JagGeo::ellipseWithinPolygon( double px0, double py0, double a0, double b0, double nx0,
                                	const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict )
{
	if ( ! validDirection(nx0) ) return false;
	double bbx, bby, rx, ry;
	getPolygonBound( mk2, sp2, bbx, bby, rx, ry );
	if (  bound2DDisjoint( px0, py0, a0,b0, bbx, bby, rx, ry ) ) { return false; }

	JagPolygon pgon;
	int rc = JagParser::addPolygonData( pgon, sp2, false );
	if ( rc < 0 ) return false;
	JagVector<JagPoint2D> vec;
	samplesOn2DEllipse( px0, py0, a0, b0, nx0, NUM_SAMPLE, vec );
	for ( int i=0; i <vec.size(); ++i ) {
		if ( ! pointWithinPolygon( vec[i].x, vec[i].y, pgon ) ) { return false; }
	}
	return true;
}

///////////////////////////////////// rectangle 3D /////////////////////////////////
bool JagGeo::rectangle3DWithinCube(  double px0, double py0, double pz0, double a0, double b0, double nx0, double ny0,
						        double x, double y, double z, double r, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, a0,b0,b0, x, y, z, r,r,r ) ) { return false; }

	JagPoint3D point[4];
	point[0].x = -a0; point[0].y = -b0; point[0].z = 0.0;
	point[1].x = -a0; point[1].y = b0; point[1].z = 0.0;
	point[2].x = a0; point[2].y = b0; point[2].z = 0.0;
	point[3].x = a0; point[3].y = -b0; point[3].z = 0.0;
	double sq_x, sq_y, sq_z, px, py, pz, loc_x, loc_y, loc_z;
	for ( int i=0; i < 4; ++i ) {
		// sq_x, sq_y, sq_z are coords in x-y-z of each cube corner node
		transform3DCoordLocal2Global( px0, py0, pz0, point[i].x, point[i].y, point[i].z, nx0, ny0, sq_x, sq_y, sq_z );
		transform3DCoordGlobal2Local( x, y, z, sq_x, sq_y, sq_z, nx, ny, loc_x, loc_y, loc_z );
		// loc_x, loc_y, loc_z are within second cube sys
		if ( ! locIn3DCenterBox( loc_x, loc_y, loc_z, r, r, r, strict ) ) { return false; }
	}
	return true;
}


bool JagGeo::rectangle3DWithinBox(  double px0, double py0, double pz0, double a0, double b0,
								double nx0, double ny0,
						        double x, double y, double z, 
							    double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, a0,b0,b0, x, y, z, w,d,h ) ) { return false; }
	JagPoint3D point[4];
	point[0].x = -a0; point[0].y = -b0; point[0].z = 0.0;
	point[1].x = -a0; point[1].y = b0; point[1].z = 0.0;
	point[2].x = a0; point[2].y = b0; point[2].z = 0.0;
	point[3].x = a0; point[3].y = -b0; point[3].z = 0.0;
	double sq_x, sq_y, sq_z, loc_x, loc_y, loc_z;
	for ( int i=0; i < 4; ++i ) {
		// sq_x, sq_y, sq_z are coords in x-y-z of each cube corner node
		transform3DCoordLocal2Global( px0, py0, pz0, point[i].x, point[i].y, point[i].z, nx0, ny0, sq_x, sq_y, sq_z );
		transform3DCoordGlobal2Local( x, y, z, sq_x, sq_y, sq_z, nx, ny, loc_x, loc_y, loc_z );
		// loc_x, loc_y, loc_z are within second cube sys
		if ( ! locIn3DCenterBox( loc_x, loc_y, loc_z, w, d, h, strict ) ) { return false; }
	}

	return true;
}

bool JagGeo::rectangle3DWithinSphere(  double px0, double py0, double pz0, double a0, double b0,
									   double nx0, double ny0,
                                	   double x, double y, double z, double r, bool strict )
{
	if (  bound3DDisjoint( px0, py0, pz0, a0,b0,b0, x, y, z, r,r,r ) ) { return false; }
	double sq_x[4], sq_y[4], sq_z[4];
	transform3DCoordLocal2Global( px0, py0, pz0, -a0, -b0, 0.0, nx0, ny0, sq_x[0], sq_y[0], sq_z[0] );
	transform3DCoordLocal2Global( px0, py0, pz0, -a0, b0, 0.0,  nx0, ny0, sq_x[1], sq_y[1], sq_z[1] );
	transform3DCoordLocal2Global( px0, py0, pz0, a0, b0, 0.0,   nx0, ny0, sq_x[2], sq_y[2], sq_z[2] );
	transform3DCoordLocal2Global( px0, py0, pz0, a0, -b0, 0.0,  nx0, ny0, sq_x[3], sq_y[3], sq_z[3] );
	for ( int i=0; i <4; ++i ) {
		if ( ! point3DWithinSphere( sq_x[i], sq_y[i], sq_z[i], x, y, z, r, strict ) ) {
			return false;
		}
	}
	return true;
}

bool JagGeo::rectangle3DWithinEllipsoid(  double px0, double py0, double pz0, double a0, double b0,
									double nx0, double ny0,
						        	double x, double y, double z, 
							    	double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, a0,b0,b0, x, y, z, w,d,h ) ) { return false; }
	JagPoint3D point[4];
	point[0].x = -a0; point[0].y = -b0; point[0].z = 0.0;
	point[1].x = -a0; point[1].y = b0; point[1].z = 0.0;
	point[2].x = a0; point[2].y = b0; point[2].z = 0.0;
	point[3].x = a0; point[3].y = -b0; point[3].z = 0.0;
	double sq_x, sq_y, sq_z, loc_x, loc_y, loc_z;
	for ( int i=0; i < 4; ++i ) {
		// sq_x, sq_y, sq_z are coords in x-y-z of each cube corner node
		transform3DCoordLocal2Global( px0, py0, pz0, point[i].x, point[i].y, point[i].z, nx0, ny0, sq_x, sq_y, sq_z );
		transform3DCoordGlobal2Local( x, y, z, sq_x, sq_y, sq_z, nx, ny, loc_x, loc_y, loc_z );
		// loc_x, loc_y, loc_z are within second cube sys
		if ( ! point3DWithinNormalEllipsoid( loc_x, loc_y, loc_z, w,d,h, strict ) ) {
			return false;
		}
	}
	return true;
}

bool JagGeo::rectangle3DWithinCone(  double px0, double py0, double pz0, double a0, double b0,
									double nx0, double ny0,
						        	double x, double y, double z, 
							    	double r, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, a0,b0,b0, x, y, z, r,r,h ) ) { return false; }

	JagPoint3D point[4];
	point[0].x = -a0; point[0].y = -b0; point[0].z = 0.0;
	point[1].x = -a0; point[1].y = b0; point[1].z = 0.0;
	point[2].x = a0; point[2].y = b0; point[2].z = 0.0;
	point[3].x = a0; point[3].y = -b0; point[3].z = 0.0;
	double sq_x, sq_y, sq_z, loc_x, loc_y, loc_z;
	for ( int i=0; i < 4; ++i ) {
		// sq_x, sq_y, sq_z are coords in x-y-z of each cube corner node
		transform3DCoordLocal2Global( px0, py0, pz0, point[i].x, point[i].y, point[i].z, nx0, ny0, sq_x, sq_y, sq_z );
		transform3DCoordGlobal2Local( x, y, z, sq_x, sq_y, sq_z, nx, ny, loc_x, loc_y, loc_z );
		// loc_x, loc_y, loc_z are within second cube sys
		if ( ! point3DWithinCone(  sq_x, sq_y, sq_z, x,y,z, r,h, nx, ny, strict ) ) {
			return false;
		}
	}
	return true;
}


///////////////////////////////////// line 3D /////////////////////////////////
bool JagGeo::line3DWithinLineString3D( double x10, double y10, double z10, double x20, double y20, double z20,
 								   const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict )
{
	// 2 points are some two neighbor points in sp2
	int start = 0;
	if ( mk2 == JAG_OJAG ) {
		start = 1;
	}

    double dx1, dy1, dz1, dx2, dy2, dz2;
    const char *str;
    char *p;
	//prt(("s6790 start=%d len=%d  square: x0=%f y0=%f r=%f\n", start, sp1.length(), x0,y0,r ));
	for ( int i=start; i < sp2.length()-1; ++i ) {
		str = sp2[i].c_str();
		if ( strchrnum( str, ':') < 2 ) continue;
		get3double(str, p, ':', dx1, dy1, dz1 );
		str = sp2[i+1].c_str();
		if ( strchrnum( str, ':') < 2 ) continue;
		get3double(str, p, ':', dx2, dy2, dz2 );

		if ( jagEQ(x10, dx1) && jagEQ(y10, dy1)  && jagEQ(z10, dz1)
			 && jagEQ(x20, dx2) && jagEQ(y20, dy2) && jagEQ( z20, dz2 ) ) {
			return true;
		}
		if ( jagEQ(x20, dx1) && jagEQ(y20, dy1) && jagEQ(z20, dz1)
		     && jagEQ(x10, dx2) && jagEQ(y10, dy2) && jagEQ(z10, dz2 ) ) {
			return true;
		}
	}
	return false;
}
 bool JagGeo::line3DWithinCube(  double x10, double y10, double z10, 
 									double x20, double y20, double z20, 
 								    double x0, double y0, double z0, double r, double nx, double ny, bool strict )

{
	if ( ! validDirection(nx, ny) ) return false;
	if ( bound3DLineBoxDisjoint( x10, y10, z10, x20, y20, z20, x0, y0, z0, r,r,r ) ) { return false; }
	double tri_x[2], tri_y[2], tri_z[2];
	tri_x[0] = x10; tri_y[0] = y10; tri_z[0] = z10;
	tri_x[1] = x20; tri_y[1] = y20; tri_z[1] = z20;
	double loc_x, loc_y, loc_z;
	for ( int i=0; i < 2; ++i ) {
		// sq_x, sq_y, sq_z are coords in x-y-z of each cube corner node
		transform3DCoordGlobal2Local( x0, y0, z0, tri_x[i], tri_y[i], tri_z[i], nx, ny, loc_x, loc_y, loc_z );
		// loc_x, loc_y, loc_z are within second cube sys
		if ( ! locIn3DCenterBox( loc_x, loc_y, loc_z, r, r, r, strict ) ) { return false; }
	}

	return true;
}


bool JagGeo::line3DWithinBox(  double x10, double y10, double z10, 
								   double x20, double y20, double z20, 
									double x0, double y0, double z0,
									double w, double d, double h, 
									double nx, double ny, bool strict )
{
	if ( ! validDirection(nx, ny) ) return false;
	if ( bound3DLineBoxDisjoint( x10, y10, z10, x20, y20, z20, x0, y0, z0, w,d,h ) ) { return false; }
	double tri_x[2], tri_y[2], tri_z[2];
	tri_x[0] = x10; tri_y[0] = y10; tri_z[0] = z10;
	tri_x[1] = x20; tri_y[1] = y20; tri_z[1] = z20;
	double loc_x, loc_y, loc_z;
	for ( int i=0; i < 2; ++i ) {
		// sq_x, sq_y, sq_z are coords in x-y-z of each cube corner node
		transform3DCoordGlobal2Local( x0, y0, z0, tri_x[i], tri_y[i], tri_z[i], nx, ny, loc_x, loc_y, loc_z );
		// loc_x, loc_y, loc_z are within second cube sys
		if ( ! locIn3DCenterBox( loc_x, loc_y, loc_z, w, d, h, strict ) ) { return false; }
	}
	return true;
}

bool JagGeo::line3DWithinSphere(  double x10, double y10, double z10, double x20, double y20, double z20, 
 									  double x, double y, double z, double r, bool strict )
{
	if ( bound3DLineBoxDisjoint( x10, y10, z10, x20, y20, z20, x, y, z, r,r,r ) ) { return false; }
	double tri_x[2], tri_y[2], tri_z[2];
	tri_x[0] = x10; tri_y[0] = y10; tri_z[0] = z10;
	tri_x[1] = x20; tri_y[1] = y20; tri_z[1] = z20;
	for ( int i=0; i < 2; ++i ) {
		if ( ! point3DWithinSphere( tri_x[i], tri_y[i], tri_z[i], x, y, z, r, strict ) ) { return false; }
	}
	return true;
}


bool JagGeo::line3DWithinEllipsoid(  double x10, double y10, double z10, double x20, double y20, double z20,
								 double x0, double y0, double z0,
								double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx, ny) ) return false;
	if ( bound3DLineBoxDisjoint( x10, y10, z10, x20, y20, z20, x0, y0, z0, w,d,h ) ) { return false; }
	double tri_x[2], tri_y[2], tri_z[2];
	tri_x[0] = x10; tri_y[0] = y10; tri_z[0] = z10;
	tri_x[1] = x20; tri_y[1] = y20; tri_z[1] = z20;
	double loc_x, loc_y, loc_z;
	for ( int i=0; i < 2; ++i ) {
		// sq_x, sq_y, sq_z are coords in x-y-z of each cube corner node
		transform3DCoordGlobal2Local( x0, y0, z0, tri_x[i], tri_y[i], tri_z[i], nx, ny, loc_x, loc_y, loc_z );
		// loc_x, loc_y, loc_z are within second cube sys
		if ( ! point3DWithinNormalEllipsoid( loc_x, loc_y, loc_z, w,d,h, strict ) ) { return false; }
	}
	return true;
}

bool JagGeo::line3DWithinCone(  double x10, double y10, double z10, double x20, double y20, double z20,
								 double x0, double y0, double z0,
								double r, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx, ny) ) return false;
	if ( bound3DLineBoxDisjoint( x10, y10, z10, x20, y20, z20, x0, y0, z0, r,r,h ) ) { return false; }

	double tri_x[2], tri_y[2], tri_z[2];
	tri_x[0] = x10; tri_y[0] = y10; tri_z[0] = z10;
	tri_x[1] = x20; tri_y[1] = y20; tri_z[1] = z20;
	for ( int i=0; i < 2; ++i ) {
		if ( ! point3DWithinCone( tri_x[i], tri_y[i], tri_z[i], x0,y0,z0, r, h, nx, ny, strict ) ) {
			return false;
		}
	}
	return true;
}


///////////////////////////////////// linestring 3D /////////////////////////////////
bool JagGeo::lineString3DWithinLineString3D(  const AbaxDataString &mk1, const JagStrSplit &sp1,
											const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict )

{
	int start1 = 0;
	double bbx1, bby1, bbz1, brx1, bry1, brz1;
	if ( mk1 == JAG_OJAG ) {
		boundingBox3DRegion(sp1[0], bbx1, bby1, bbz1, brx1, bry1, brz1 );
		start1 = 1;
	}

	int start2 = 0;
	double bbx2, bby2, bbz2, brx2, bry2, brz2;
	if ( mk2 == JAG_OJAG ) {
		boundingBox3DRegion(sp2[0], bbx2, bby2, bbz2, brx2, bry2, brz2 );
		start2 = 1;
	}

	if ( bound3DDisjoint( bbx1, bby1, bbz1, brx1, bry1, brz1, bbx2, bby2, bbz2, brx2, bry2, brz2 ) ) {
		return false;
	}

	// assume sp1 has fewer lines than sp2
	if ( strict ) {
		if ( sp1.length() - start1 >= sp2.length() - start2 ) return false;
	} else {
		if ( sp1.length() - start1 > sp2.length() - start2 ) return false;
	}

	int rc = KMPPointsWithin( sp1, start1, sp2, start2 );
	if ( rc < 0 ) return false;
	return true;
}

bool JagGeo::lineString3DWithinCube( const AbaxDataString &mk1, const JagStrSplit &sp1,
 								    double x0, double y0, double z0, double r, double nx, double ny, bool strict )

{
	if ( ! validDirection(nx, ny) ) return false;
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        start = 1;
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  r, r, r ) ) {
            //prt(("s6770 false\n" ));
            return false;
        }
    }

    double loc_x, loc_y, loc_z;
    double dx, dy, dz;
    const char *str;
    char *p;
    for ( int i=start; i < sp1.length(); ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx, dy, dz );
        transform3DCoordGlobal2Local( x0, y0, z0, dx, dy, dz, nx, ny, loc_x, loc_y, loc_z );
        if ( ! locIn3DCenterBox( loc_x, loc_y, loc_z,  r,r,r, strict ) ) { return false; }
    }
	return true;
}


bool JagGeo::lineString3DWithinBox( const AbaxDataString &mk1, const JagStrSplit &sp1,
									double x0, double y0, double z0,
									double w, double d, double h, 
									double nx, double ny, bool strict )
{
	if ( ! validDirection(nx, ny) ) return false;
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        start = 1;
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  w, d, h ) ) {
            //prt(("s6770 false\n" ));
            return false;
        }
    }

    double loc_x, loc_y, loc_z;
    double dx, dy, dz;
    const char *str;
    char *p;
    for ( int i=start; i < sp1.length(); ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx, dy, dz );
        transform3DCoordGlobal2Local( x0, y0, z0, dx, dy, dz, nx, ny, loc_x, loc_y, loc_z );
        if ( ! locIn3DCenterBox( loc_x, loc_y, loc_z,  w,d,h, strict ) ) { return false; }
    }
	return true;

}

bool JagGeo::lineString3DWithinSphere(  const AbaxDataString &mk1, const JagStrSplit &sp1,
 									  double x0, double y0, double z0, double r, bool strict )
{
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        start = 1;
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  r, r, r ) ) {
            //prt(("s6770 false\n" ));
            return false;
        }
    }

    double dx, dy, dz;
    const char *str;
    char *p;
    for ( int i=start; i < sp1.length(); ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx, dy, dz );
		if ( ! point3DWithinSphere( dx, dy, dz, x0, y0, z0, r, strict ) ) { return false; }
    }
	return true;
}


bool JagGeo::lineString3DWithinEllipsoid(  const AbaxDataString &mk1, const JagStrSplit &sp1,
								 double x0, double y0, double z0,
								double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx, ny) ) return false;
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        start = 1;
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  w, d, h ) ) {
            //prt(("s6770 false\n" ));
            return false;
        }
    }

    double dx, dy, dz;
    double loc_x, loc_y, loc_z;
    const char *str;
    char *p;
    for ( int i=start; i < sp1.length(); ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx, dy, dz );
		transform3DCoordGlobal2Local( x0, y0, z0, dx, dy, dz, nx, ny, loc_x, loc_y, loc_z );
		if ( ! point3DWithinNormalEllipsoid( loc_x, loc_y, loc_z, w,d,h, strict ) ) { return false; }
    }
	return true;
}

bool JagGeo::lineString3DWithinCone(  const AbaxDataString &mk1, const JagStrSplit &sp1,
								 double x0, double y0, double z0,
								double r, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx, ny) ) return false;
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        start = 1;
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  r, r, h ) ) {
            //prt(("s6770 false\n" ));
            return false;
        }
    }

    double dx, dy, dz;
    const char *str;
    char *p;
    for ( int i=start; i < sp1.length(); ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx, dy, dz );
		if ( ! point3DWithinCone( dx, dy, dz, x0,y0,z0, r, h, nx, ny, strict ) ) { return false; }
    }
	return true;
}


///////////////////////////////////// polygon 3D /////////////////////////////////

bool JagGeo::polygon3DWithinCube( const AbaxDataString &mk1, const JagStrSplit &sp1,
 								    double x0, double y0, double z0, double r, double nx, double ny, bool strict )

{
	if ( ! validDirection(nx, ny) ) return false;
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        start = 1;
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  r, r, r ) ) {
            //prt(("s6770 false\n" ));
            return false;
        }
    }

	//prt(("s1029 polygon3DWithinCube() sp1:\n"));
	//sp1.print();

    double loc_x, loc_y, loc_z;
    double dx, dy, dz;
    const char *str;
    char *p;
    for ( int i=start; i < sp1.length(); ++i ) {
		if ( sp1[i] == "|" || sp1[i] == "!" ) break;
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx, dy, dz );
        transform3DCoordGlobal2Local( x0, y0, z0, dx, dy, dz, nx, ny, loc_x, loc_y, loc_z );
        if ( ! locIn3DCenterBox( loc_x, loc_y, loc_z,  r,r,r, strict ) ) { return false; }
    }
	return true;
}


bool JagGeo::polygon3DWithinBox( const AbaxDataString &mk1, const JagStrSplit &sp1,
									double x0, double y0, double z0,
									double w, double d, double h, 
									double nx, double ny, bool strict )
{
	if ( ! validDirection(nx, ny) ) return false;
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        start = 1;
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  w, d, h ) ) {
            //prt(("s6770 false\n" ));
            return false;
        }
    }

    double loc_x, loc_y, loc_z;
    double dx, dy, dz;
    const char *str;
    char *p;
    for ( int i=start; i < sp1.length(); ++i ) {
		if ( sp1[i] == "|" || sp1[i] == "!" ) break;
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx, dy, dz );
        transform3DCoordGlobal2Local( x0, y0, z0, dx, dy, dz, nx, ny, loc_x, loc_y, loc_z );
        if ( ! locIn3DCenterBox( loc_x, loc_y, loc_z,  w,d,h, strict ) ) { return false; }
    }
	return true;

}

bool JagGeo::polygon3DWithinSphere(  const AbaxDataString &mk1, const JagStrSplit &sp1,
 									  double x0, double y0, double z0, double r, bool strict )
{
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        start = 1;
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  r, r, r ) ) {
            //prt(("s6770 false\n" ));
            return false;
        }
    }

    double dx, dy, dz;
    const char *str;
    char *p;
    for ( int i=start; i < sp1.length(); ++i ) {
		if ( sp1[i] == "|" || sp1[i] == "!" ) break;
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx, dy, dz );
		if ( ! point3DWithinSphere( dx, dy, dz, x0, y0, z0, r, strict ) ) { return false; }
    }
	return true;
}


bool JagGeo::polygon3DWithinEllipsoid(  const AbaxDataString &mk1, const JagStrSplit &sp1,
								 double x0, double y0, double z0,
								double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx, ny) ) return false;
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        start = 1;
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  w, d, h ) ) {
            //prt(("s6770 false\n" ));
            return false;
        }
    }

    double dx, dy, dz;
    double loc_x, loc_y, loc_z;
    const char *str;
    char *p;
    for ( int i=start; i < sp1.length(); ++i ) {
		if ( sp1[i] == "|" || sp1[i] == "!" ) break;
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx, dy, dz );
		transform3DCoordGlobal2Local( x0, y0, z0, dx, dy, dz, nx, ny, loc_x, loc_y, loc_z );
		if ( ! point3DWithinNormalEllipsoid( loc_x, loc_y, loc_z, w,d,h, strict ) ) { return false; }
    }
	return true;
}

bool JagGeo::polygon3DWithinCone(  const AbaxDataString &mk1, const JagStrSplit &sp1,
								 double x0, double y0, double z0,
								double r, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx, ny) ) return false;
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        start = 1;
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  r, r, h ) ) {
            //prt(("s6770 false\n" ));
            return false;
        }
    }

    double dx, dy, dz;
    const char *str;
    char *p;
    for ( int i=start; i < sp1.length(); ++i ) {
		if ( sp1[i] == "|" || sp1[i] == "!" ) break;
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx, dy, dz );
		if ( ! point3DWithinCone( dx, dy, dz, x0,y0,z0, r, h, nx, ny, strict ) ) { return false; }
    }
	return true;
}


///////////////////////// multiPolygon3DWithin /////////////////////////////////////////////////
bool JagGeo::multiPolygon3DWithinCube( const AbaxDataString &mk1, const JagStrSplit &sp1,
 								    double x0, double y0, double z0, double r, double nx, double ny, bool strict )

{
	if ( ! validDirection(nx, ny) ) return false;
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        start = 1;
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  r, r, r ) ) {
            //prt(("s6770 false\n" ));
            return false;
        }
    }

    double loc_x, loc_y, loc_z;
    double dx, dy, dz;
    const char *str;
    char *p;
	bool skip = false;
    for ( int i=start; i < sp1.length(); ++i ) {
        if ( sp1[i] == "|" ) {  skip = true; }
        else if ( sp1[i] == "!" ) {  skip = false; }
        else {
            if ( skip ) continue;
        	str = sp1[i].c_str();
        	if ( strchrnum( str, ':') < 2 ) continue;
        	get3double(str, p, ':', dx, dy, dz );
        	transform3DCoordGlobal2Local( x0, y0, z0, dx, dy, dz, nx, ny, loc_x, loc_y, loc_z );
        	if ( ! locIn3DCenterBox( loc_x, loc_y, loc_z,  r,r,r, strict ) ) { return false; }
		}
    }
	return true;
}


bool JagGeo::multiPolygon3DWithinBox( const AbaxDataString &mk1, const JagStrSplit &sp1,
									double x0, double y0, double z0,
									double w, double d, double h, 
									double nx, double ny, bool strict )
{
	if ( ! validDirection(nx, ny) ) return false;
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        start = 1;
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  w, d, h ) ) {
            //prt(("s6770 false\n" ));
            return false;
        }
    }

    double loc_x, loc_y, loc_z;
    double dx, dy, dz;
    const char *str;
    char *p;
	bool skip = false;
    for ( int i=start; i < sp1.length(); ++i ) {
        if ( sp1[i] == "|" ) {  skip = true; }
        else if ( sp1[i] == "!" ) {  skip = false; }
        else {
            if ( skip ) continue;
        	str = sp1[i].c_str();
        	if ( strchrnum( str, ':') < 2 ) continue;
        	get3double(str, p, ':', dx, dy, dz );
        	transform3DCoordGlobal2Local( x0, y0, z0, dx, dy, dz, nx, ny, loc_x, loc_y, loc_z );
        	if ( ! locIn3DCenterBox( loc_x, loc_y, loc_z,  w,d,h, strict ) ) { return false; }
		}
    }
	return true;

}

bool JagGeo::multiPolygon3DWithinSphere(  const AbaxDataString &mk1, const JagStrSplit &sp1,
 									  double x0, double y0, double z0, double r, bool strict )
{
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        start = 1;
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  r, r, r ) ) {
            //prt(("s6770 false\n" ));
            return false;
        }
    }

    double dx, dy, dz;
    const char *str;
    char *p;
	bool skip = false;
    for ( int i=start; i < sp1.length(); ++i ) {
		// if ( sp1[i] == "|" || sp1[i] == "!" ) break;
        if ( sp1[i] == "|" ) {  skip = true; }
        else if ( sp1[i] == "!" ) {  skip = false; }
        else {
            if ( skip ) continue;
        	str = sp1[i].c_str();
        	if ( strchrnum( str, ':') < 2 ) continue;
        	get3double(str, p, ':', dx, dy, dz );
			if ( ! point3DWithinSphere( dx, dy, dz, x0, y0, z0, r, strict ) ) { return false; }
		}
    }
	return true;
}


bool JagGeo::multiPolygon3DWithinEllipsoid(  const AbaxDataString &mk1, const JagStrSplit &sp1,
								 double x0, double y0, double z0,
								double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx, ny) ) return false;
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        start = 1;
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  w, d, h ) ) {
            //prt(("s6770 false\n" ));
            return false;
        }
    }

    double dx, dy, dz;
    double loc_x, loc_y, loc_z;
    const char *str;
    char *p;
	bool skip = false;
    for ( int i=start; i < sp1.length(); ++i ) {
		// if ( sp1[i] == "|" || sp1[i] == "!" ) break;
        if ( sp1[i] == "|" ) {  skip = true; }
        else if ( sp1[i] == "!" ) {  skip = false; }
        else {
            if ( skip ) continue;
        	str = sp1[i].c_str();
        	if ( strchrnum( str, ':') < 2 ) continue;
        	get3double(str, p, ':', dx, dy, dz );
			transform3DCoordGlobal2Local( x0, y0, z0, dx, dy, dz, nx, ny, loc_x, loc_y, loc_z );
			if ( ! point3DWithinNormalEllipsoid( loc_x, loc_y, loc_z, w,d,h, strict ) ) { return false; }
		}
    }
	return true;
}

bool JagGeo::multiPolygon3DWithinCone(  const AbaxDataString &mk1, const JagStrSplit &sp1,
								 double x0, double y0, double z0,
								double r, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx, ny) ) return false;
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        start = 1;
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  r, r, h ) ) {
            //prt(("s6770 false\n" ));
            return false;
        }
    }

    double dx, dy, dz;
    const char *str;
    char *p;
	bool skip = false;
    for ( int i=start; i < sp1.length(); ++i ) {
		//if ( sp1[i] == "|" || sp1[i] == "!" ) break;
        if ( sp1[i] == "|" ) {  skip = true; }
        else if ( sp1[i] == "!" ) {  skip = false; }
        else {
            if ( skip ) continue;
        	str = sp1[i].c_str();
        	if ( strchrnum( str, ':') < 2 ) continue;
        	get3double(str, p, ':', dx, dy, dz );
			if ( ! point3DWithinCone( dx, dy, dz, x0,y0,z0, r, h, nx, ny, strict ) ) { return false; }
		}
    }
	return true;
}


///////////////////////////////////// triangle 3D /////////////////////////////////
 bool JagGeo::triangle3DWithinCube(  double x10, double y10, double z10, 
 									double x20, double y20, double z20, 
									double x30, double y30, double z30,
 								    double x0, double y0, double z0, double r, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx, ny) ) return false;
	if ( bound3DLineBoxDisjoint( x10, y10, z10, x20, y20, z20, x0, y0, z0, r,r,r )
		&& bound3DLineBoxDisjoint( x20, y20, z20, x30, y30, z30, x0, y0, z0, r,r,r )
		&& bound3DLineBoxDisjoint( x30, y30, z30, x10, y10, z10, x0, y0, z0, r,r,r ) )
	{
		return false;
	}

	double tri_x[3], tri_y[3], tri_z[3];
	tri_x[0] = x10; tri_y[0] = y10; tri_z[0] = z10;
	tri_x[1] = x20; tri_y[1] = y20; tri_z[1] = z20;
	tri_x[2] = x30; tri_y[2] = y30; tri_z[2] = z30;
	double loc_x, loc_y, loc_z;
	for ( int i=0; i < 3; ++i ) {
		// sq_x, sq_y, sq_z are coords in x-y-z of each cube corner node
		transform3DCoordGlobal2Local( x0, y0, z0, tri_x[i], tri_y[i], tri_z[i], nx, ny, loc_x, loc_y, loc_z );
		if ( ! locIn3DCenterBox( loc_x, loc_y, loc_z, r, r, r, strict ) ) { return false; }
	}

	return true;
}

bool JagGeo::triangle3DWithinBox(  double x10, double y10, double z10, 
								   double x20, double y20, double z20, 
									double x30, double y30, double z30,
									double x0, double y0, double z0,
									double w, double d, double h, 
									double nx, double ny, bool strict )
{
	if ( ! validDirection(nx, ny) ) return false;
	if ( bound3DLineBoxDisjoint( x10, y10, z10, x20, y20, z20, x0, y0, z0, w,d,h )
		&& bound3DLineBoxDisjoint( x20, y20, z20, x30, y30, z30, x0, y0, z0, w,d,h )
		&& bound3DLineBoxDisjoint( x30, y30, z30, x10, y10, z10, x0, y0, z0, w,d,h ) )
	{
		return false;
	}

	double tri_x[3], tri_y[3], tri_z[3];
	tri_x[0] = x10; tri_y[0] = y10; tri_z[0] = z10;
	tri_x[1] = x20; tri_y[1] = y20; tri_z[1] = z20;
	tri_x[2] = x30; tri_y[2] = y30; tri_z[2] = z30;
	double loc_x, loc_y, loc_z;
	for ( int i=0; i < 3; ++i ) {
		// sq_x, sq_y, sq_z are coords in x-y-z of each cube corner node
		transform3DCoordGlobal2Local( x0, y0, z0, tri_x[i], tri_y[i], tri_z[i], nx, ny, loc_x, loc_y, loc_z );
		if ( ! locIn3DCenterBox( loc_x, loc_y, loc_z, w, d, h, strict ) ) { return false; }
	}
	return true;
}

bool JagGeo::triangle3DWithinSphere(  double x10, double y10, double z10, double x20, double y20, double z20, 
										double x30, double y30, double z30,
 									  double x, double y, double z, double r, bool strict )
{
	if ( bound3DLineBoxDisjoint( x10, y10, z10, x20, y20, z20, x, y, z, r,r,r )
		&& bound3DLineBoxDisjoint( x20, y20, z20, x30, y30, z30, x, y, z, r,r,r )
		&& bound3DLineBoxDisjoint( x30, y30, z30, x10, y10, z10, x, y, z, r,r,r ) )
	{
		return false;
	}

	double tri_x[3], tri_y[3], tri_z[3];
	tri_x[0] = x10; tri_y[0] = y10; tri_z[0] = z10;
	tri_x[1] = x20; tri_y[1] = y20; tri_z[1] = z20;
	tri_x[2] = x30; tri_y[2] = y30; tri_z[2] = z30;
	for ( int i=0; i < 3; ++i ) {
		if ( ! point3DWithinSphere( tri_x[i], tri_y[i], tri_z[i], x, y, z, r, strict ) ) { return false; }
	}
	return true;
}


bool JagGeo::triangle3DWithinEllipsoid(  double x10, double y10, double z10, double x20, double y20, double z20,
										double x30, double y30, double z30,
								 double x0, double y0, double z0,
								double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx, ny) ) return false;
	if ( bound3DLineBoxDisjoint( x10, y10, z10, x20, y20, z20, x0, y0, z0, w,d,h )
		&& bound3DLineBoxDisjoint( x20, y20, z20, x30, y30, z30, x0, y0, z0, w,d,h )
		&& bound3DLineBoxDisjoint( x30, y30, z30, x10, y10, z10, x0, y0, z0, w,d,h ) )
	{
		return false;
	}

	double tri_x[3], tri_y[3], tri_z[3];
	tri_x[0] = x10; tri_y[0] = y10; tri_z[0] = z10;
	tri_x[1] = x20; tri_y[1] = y20; tri_z[1] = z20;
	tri_x[2] = x30; tri_y[2] = y30; tri_z[2] = z30;
	double loc_x, loc_y, loc_z;
	for ( int i=0; i < 3; ++i ) {
		// sq_x, sq_y, sq_z are coords in x-y-z of each cube corner node
		transform3DCoordGlobal2Local( x0, y0, z0, tri_x[i], tri_y[i], tri_z[i], nx, ny, loc_x, loc_y, loc_z );
		if ( ! point3DWithinNormalEllipsoid( loc_x, loc_y, loc_z, w,d,h, strict ) ) { return false; }
	}
	return true;
}


bool JagGeo::triangle3DWithinCone(  double x10, double y10, double z10, double x20, double y20, double z20,
										double x30, double y30, double z30,
								 double x0, double y0, double z0,
								double r, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx, ny) ) return false;
	//if (  bound3DDisjoint( px0, py0, pz0, a0,b0,c0, x, y, z, w,d,h ) ) { return false; }
	if ( bound3DLineBoxDisjoint( x10, y10, z10, x20, y20, z20, x0, y0, z0, r,r,h )
		&& bound3DLineBoxDisjoint( x20, y20, z20, x30, y30, z30, x0, y0, z0, r,r,h )
		&& bound3DLineBoxDisjoint( x30, y30, z30, x10, y10, z10, x0, y0, z0, r,r,h ) )
	{
		return false;
	}

	double tri_x[3], tri_y[3], tri_z[3];
	tri_x[0] = x10; tri_y[0] = y10; tri_z[0] = z10;
	tri_x[1] = x20; tri_y[1] = y20; tri_z[1] = z20;
	tri_x[2] = x30; tri_y[2] = y30; tri_z[2] = z30;
	for ( int i=0; i < 3; ++i ) {
		if ( ! point3DWithinCone( tri_x[i], tri_y[i], tri_z[i], x0,y0,z0, r, h, nx, ny, strict ) ) {
			return false;
		}
	}
	return true;
}

/////////////////////////////////////////////// 3D box within
bool JagGeo::boxWithinCube(  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0,
						        double x, double y, double z, double r, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, a0,b0,b0, x, y, z, r,r,r ) ) { return false; }

	JagPoint3D point[8];
	point[0].x = -a0; point[0].y = -b0; point[0].z = -c0;
	point[1].x = -a0; point[1].y = b0; point[1].z = -c0;
	point[2].x = a0; point[2].y = b0; point[2].z = -c0;
	point[3].x = a0; point[3].y = -b0; point[3].z = -c0;
	point[4].x = -a0; point[4].y = -b0; point[4].z = c0;
	point[5].x = -a0; point[5].y = b0; point[5].z = c0;
	point[6].x = a0; point[6].y = b0; point[6].z = c0;
	point[7].x = a0; point[7].y = -b0; point[7].z = c0;

	double sq_x, sq_y, sq_z, loc_x, loc_y, loc_z;
	for ( int i=0; i < 8; ++i ) {
		// sq_x, sq_y, sq_z are coords in x-y-z of each cube corner node
		transform3DCoordLocal2Global( px0, py0, pz0, point[i].x, point[i].y, point[i].z, nx0, ny0, sq_x, sq_y, sq_z );
		transform3DCoordGlobal2Local( x, y, z, sq_x, sq_y, sq_z, nx, ny, loc_x, loc_y, loc_z );
		// loc_x, loc_y, loc_z are within second cube sys
		if ( ! locIn3DCenterBox( loc_x, loc_y, loc_z, r, r, r, strict ) ) { return false; }
	}
	return true;
}


bool JagGeo::boxWithinBox(  double px0, double py0, double pz0, double a0, double b0, double c0,
								double nx0, double ny0,
						        double x, double y, double z, 
							    double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, a0,b0,c0, x, y, z, w,d,h ) ) { return false; }

	JagPoint3D point[8];
	point[0].x = -a0; point[0].y = -b0; point[0].z = -c0;
	point[1].x = -a0; point[1].y = b0; point[1].z = -c0;
	point[2].x = a0; point[2].y = b0; point[2].z = -c0;
	point[3].x = a0; point[3].y = -b0; point[3].z = -c0;
	point[4].x = -a0; point[4].y = -b0; point[4].z = c0;
	point[5].x = -a0; point[5].y = b0; point[5].z = c0;
	point[6].x = a0; point[6].y = b0; point[6].z = c0;
	point[7].x = a0; point[7].y = -b0; point[7].z = c0;

	double sq_x, sq_y, sq_z, loc_x, loc_y, loc_z;
	for ( int i=0; i < 8; ++i ) {
		// sq_x, sq_y, sq_z are coords in x-y-z of each cube corner node
		transform3DCoordLocal2Global( px0, py0, pz0, point[i].x, point[i].y, point[i].z, nx0, ny0, sq_x, sq_y, sq_z );
		transform3DCoordGlobal2Local( x, y, z, sq_x, sq_y, sq_z, nx, ny, loc_x, loc_y, loc_z );
		// loc_x, loc_y, loc_z are within second cube sys
		if ( ! locIn3DCenterBox( loc_x, loc_y, loc_z, w, d, h, strict ) ) { return false; }
	}
	return true;
}

bool JagGeo::boxWithinSphere(  double px0, double py0, double pz0, double a0, double b0, double c0,
							   double nx0, double ny0,
                           	   double x, double y, double z, double r, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, a0,b0,c0, x, y, z, r,r,r ) ) { return false; }
	//if ( ! validDirection(nx, ny) ) return false;

	//if ( ! point3DWithinSphere( sq_x8, sq_y8, sq_z8, x0, y0, z0, r, strict ) ) { return false; }
	// return true;
	JagPoint3D point[8];
	point[0].x = -a0; point[0].y = -b0; point[0].z = -c0;
	point[1].x = -a0; point[1].y = b0; point[1].z = -c0;
	point[2].x = a0; point[2].y = b0; point[2].z = -c0;
	point[3].x = a0; point[3].y = -b0; point[3].z = -c0;
	point[4].x = -a0; point[4].y = -b0; point[4].z = c0;
	point[5].x = -a0; point[5].y = b0; point[5].z = c0;
	point[6].x = a0; point[6].y = b0; point[6].z = c0;
	point[7].x = a0; point[7].y = -b0; point[7].z = c0;

	double sq_x, sq_y, sq_z, loc_x, loc_y, loc_z;
	for ( int i=0; i < 8; ++i ) {
		// sq_x, sq_y, sq_z are coords in x-y-z of each cube corner node
		transform3DCoordLocal2Global( px0, py0, pz0, point[i].x, point[i].y, point[i].z, nx0, ny0, sq_x, sq_y, sq_z );
		// loc_x, loc_y, loc_z are within second cube sys
		if ( ! point3DWithinSphere( sq_x, sq_y, sq_z, x, y, z, r, strict ) ) { return false; }
	}
	return true;
}

bool JagGeo::boxWithinEllipsoid(  double px0, double py0, double pz0, double a0, double b0, double c0,
								  double nx0, double ny0,
						        	double x0, double y0, double z0, 
							    	double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, a0,b0,c0, x0, y0, z0, w,d,h ) ) { return false; }

	JagPoint3D point[8];
	point[0].x = -a0; point[0].y = -b0; point[0].z = -c0;
	point[1].x = -a0; point[1].y = b0; point[1].z = -c0;
	point[2].x = a0; point[2].y = b0; point[2].z = -c0;
	point[3].x = a0; point[3].y = -b0; point[3].z = -c0;
	point[4].x = -a0; point[4].y = -b0; point[4].z = c0;
	point[5].x = -a0; point[5].y = b0; point[5].z = c0;
	point[6].x = a0; point[6].y = b0; point[6].z = c0;
	point[7].x = a0; point[7].y = -b0; point[7].z = c0;
	double sq_x, sq_y, sq_z, loc_x, loc_y, loc_z;
	for ( int i=0; i < 8; ++i ) {
		// sq_x, sq_y, sq_z are coords in x-y-z of each cube corner node
		transform3DCoordLocal2Global( px0, py0, pz0, point[i].x, point[i].y, point[i].z, nx0, ny0, sq_x, sq_y, sq_z );
		transform3DCoordGlobal2Local( x0, y0, z0, sq_x, sq_y, sq_z, nx, ny, loc_x, loc_y, loc_z );
		// loc_x, loc_y, loc_z are within second cube sys
		if ( ! point3DWithinNormalEllipsoid( loc_x, loc_y, loc_z, w,d,h, strict ) ) { return false; }
	}
	return true;

}

bool JagGeo::boxWithinCone(  double px0, double py0, double pz0, double a0, double b0, double c0,
								  double nx0, double ny0,
						        	double x0, double y0, double z0, 
							    	double r, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, a0,b0,b0, x0, y0, z0, r,r,h ) ) { return false; }
	JagPoint3D point[8];
	point[0].x = -a0; point[0].y = -b0; point[0].z = -c0;
	point[1].x = -a0; point[1].y = b0; point[1].z = -c0;
	point[2].x = a0; point[2].y = b0; point[2].z = -c0;
	point[3].x = a0; point[3].y = -b0; point[3].z = -c0;
	point[4].x = -a0; point[4].y = -b0; point[4].z = c0;
	point[5].x = -a0; point[5].y = b0; point[5].z = c0;
	point[6].x = a0; point[6].y = b0; point[6].z = c0;
	point[7].x = a0; point[7].y = -b0; point[7].z = c0;
	double sq_x, sq_y, sq_z, loc_x, loc_y, loc_z;
	for ( int i=0; i < 8; ++i ) {
		transform3DCoordLocal2Global( px0, py0, pz0, point[i].x, point[i].y, point[i].z, nx0, ny0, sq_x, sq_y, sq_z );
		if ( ! point3DWithinCone( sq_x, sq_y, sq_z, x0,y0,z0, r,h, nx,ny, strict ) ) { return false; }
	}
	return true;

}

// ellipsoid within
bool JagGeo::ellipsoidWithinCube(  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0,
						        double x0, double y0, double z0, double r, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, a0,b0,c0, x0, y0, z0, r,r,r ) ) { return false; }

	double loc_x, loc_y, loc_z;
	JagVector<JagPoint3D> vec;
	samplesOnEllipsoid( px0, py0, pz0, a0, b0, c0, nx0, ny0, NUM_SAMPLE, vec );
	for ( int i=0; i <vec.size(); ++i ) {
    	transform3DCoordGlobal2Local( x0, y0, z0, vec[i].x, vec[i].y, vec[i].z, nx, ny, loc_x, loc_y, loc_z );
		if ( ! locIn3DCenterBox( loc_x, loc_y, loc_z, r, r, r, strict ) ) { return false; }
	}
	return true;
}


bool JagGeo::ellipsoidWithinBox(  double px0, double py0, double pz0, double a0, double b0, double c0,
								double nx0, double ny0,
						        double x0, double y0, double z0, 
							    double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, a0,b0,b0, x0, y0, z0, w,d,h ) ) { return false; }
	JagVector<JagPoint3D> vec;
	double loc_x, loc_y, loc_z;
	samplesOnEllipsoid( px0, py0, pz0, a0, b0, c0, nx0, ny0, NUM_SAMPLE, vec );
	for ( int i=0; i <vec.size(); ++i ) {
    	transform3DCoordGlobal2Local( x0, y0, z0, vec[i].x, vec[i].y, vec[i].z, nx, ny, loc_x, loc_y, loc_z );
		if ( ! locIn3DCenterBox( loc_x, loc_y, loc_z, w, d, h, strict ) ) { return false; }
	}
	return true;
}

bool JagGeo::ellipsoidWithinSphere(  double px0, double py0, double pz0, double a0, double b0, double c0,
							   double nx0, double ny0,
                           	   double x0, double y0, double z0, double r, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, a0,b0,c0, x0, y0, z0, r,r,r ) ) { return false; }

	JagVector<JagPoint3D> vec;
	samplesOnEllipsoid( px0, py0, pz0, a0, b0, c0, nx0, ny0, NUM_SAMPLE, vec );
	for ( int i=0; i <vec.size(); ++i ) {
		if ( ! point3DWithinSphere( vec[i].x, vec[i].y, vec[i].z, x0,y0,z0, r, strict ) ) { return false; }
	}
	return true;
}

bool JagGeo::ellipsoidWithinEllipsoid(  double px0, double py0, double pz0, double a0, double b0, double c0,
								  double nx0, double ny0,
						        	double x0, double y0, double z0, 
							    	double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, a0,b0,b0, x0, y0, z0, w,d,h ) ) { return false; }
	JagVector<JagPoint3D> vec;
	double loc_x, loc_y, loc_z;
	samplesOnEllipsoid( px0, py0, pz0, a0, b0, c0, nx0, ny0, NUM_SAMPLE, vec );
	for ( int i=0; i <vec.size(); ++i ) {
    	transform3DCoordGlobal2Local( x0, y0, z0, vec[i].x, vec[i].y, vec[i].z, nx, ny, loc_x, loc_y, loc_z );
		if ( ! point3DWithinNormalEllipsoid( loc_x, loc_y, loc_z, w,d,h, strict ) ) { return false; }
	}
	return true;
}

bool JagGeo::ellipsoidWithinCone(  double px0, double py0, double pz0, double a0, double b0, double c0,
								  double nx0, double ny0,
						        	double x0, double y0, double z0, 
							    	double r, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, a0,b0,b0, x0, y0, z0, r,r,h ) ) { return false; }
	JagVector<JagPoint3D> vec;
	double loc_x, loc_y, loc_z;
	samplesOnEllipsoid( px0, py0, pz0, a0, b0, c0, nx0, ny0, NUM_SAMPLE, vec );
	for ( int i=0; i <vec.size(); ++i ) {
		if ( ! point3DWithinCone( vec[i].x, vec[i].y, vec[i].z, x0,y0,z0, r, h, nx, ny, strict ) ) {
			return false;
		}
	}
	return true;
}




///////////////////////////////////// 3D cylinder /////////////////////////////////
bool JagGeo::cylinderWithinCube(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
						        double x, double y, double z, double r, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, pr0,pr0,c0, x, y, z, r,r,r ) ) { return false; }

	#if 1
	JagPoint3D center[2];
	// upper and lower local circles of the cynliner
	center[0].x = 0.0; center[0].y = 0.0; center[0].z = c0; 
	center[1].x = 0.0; center[1].y = 0.0; center[1].z = -c0; 

	double circlenx, circleny, circlenz; // direction of nx0 ny0 in nx ny
	transform3DDirection( nx0, ny0, nx, ny, circlenx, circleny );
	circlenz = sqrt( 1.0 - circlenx*circlenx - circleny*circleny );
	double outx, outy, outz, loc_x, loc_y, loc_z, cirx, ciry, cirz, cirnx, cirny, cirnz, a, b;
	double x1, y1, x2, y2, x3, y3, x4, y4;
	for ( int i=0; i < 2; ++i ) {
		// conver from local to global
		transform3DCoordLocal2Global( px0, py0, pz0, center[i].x, center[i].y, center[i].z, nx0, ny0, outx, outy, outz );
		transform3DCoordGlobal2Local( x, y, z, outx, outy, outz, nx, ny, loc_x, loc_y, loc_z );
		// now we have 3D circle  at (loc_x, loc_y, loc_z) and direction (circlenx, circleny)

		project3DCircleToXYPlane( loc_x, loc_y, loc_z, pr0, circlenx, circleny, cirx, ciry, a, b, cirnx );
		// we have an ellipse on xy-plane in normal cube space   (cirx, ciry) (a,b) cirnx
		boundingBoxOfRotatedEllipse( cirx, ciry, a, b, cirnx, x1, y1, x2, y2, x3, y3, x4, y4 );
		if ( ! locIn2DCenterBox( x1, y1, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x2, y2, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x3, y3, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x4, y4, r, r, strict ) ) { return false; }

		project3DCircleToYZPlane( loc_x, loc_y, loc_z, pr0, circleny, circlenz, ciry, cirz, a, b, cirny );
		// we have an ellipse on xy-plane in normal cube space   (ciry, cirz) (a,b) cirny
		boundingBoxOfRotatedEllipse( ciry, cirz, a, b, cirny, x1, y1, x2, y2, x3, y3, x4, y4 );
		if ( ! locIn2DCenterBox( x1, y1, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x2, y2, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x3, y3, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x4, y4, r, r, strict ) ) { return false; }

		project3DCircleToZXPlane( loc_x, loc_y, loc_z, pr0, circlenz, circlenx, cirz, cirx, a, b, cirnz );
		// we have an ellipse on xy-plane in normal cube space   (cirz, cirx) (a,b) cirnz
		boundingBoxOfRotatedEllipse( cirz, cirx, a, b, cirnz, x1, y1, x2, y2, x3, y3, x4, y4 );
		if ( ! locIn2DCenterBox( x1, y1, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x2, y2, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x3, y3, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x4, y4, r, r, strict ) ) { return false; }

    }
	return true;

	#else

	JagPoint3D center[2];
	center[0].x = 0.0; center[0].y = 0.0; center[0].z = -c0;
	center[1].x = 0.0; center[1].y = 0.0; center[1].z = c0;
	double gx,gy,gz, locx, locy, locz;
	for ( int i=0; i < 2; ++i ) {
		JagVector<JagPoint3D> vec;
		samplesOn3DCircle( center[i].x, center[i].y, center[i].z, pr0, nx0, ny0, NUM_SAMPLE, vec );
		for ( int j=0; j < vec.size(); ++j ) {
			transform3DCoordLocal2Global( px0, py0, pz0, vec[j].x, vec[j].y, vec[j].z, nx0, ny0, gx, gy, gz );
			transform3DCoordGlobal2Local( x,y,z, gx,gy,gz, nx, ny, locx, locy, locz );
			if ( ! locIn3DCenterBox( locx, locy, locz, r,r,r, strict ) ) return false;
		}
	}
	return true;

	#endif
}


bool JagGeo::cylinderWithinBox(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
						        double x, double y, double z, 
							    double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, pr0,pr0,c0, x, y, z, w,d,h ) ) { return false; }

	#if 1
	JagPoint3D center[2];
	// upper and lower local circles of the cynliner
	center[0].x = 0.0; center[0].y = 0.0; center[0].z = c0; 
	center[1].x = 0.0; center[1].y = 0.0; center[1].z = -c0; 

	double circlenx, circleny, circlenz; // direction of nx0 ny0 in nx ny
	transform3DDirection( nx0, ny0, nx, ny, circlenx, circleny );
	circlenz = sqrt( 1.0 - circlenx*circlenx - circleny*circleny );
	double outx, outy, outz, loc_x, loc_y, loc_z, cirx, ciry, cirz, cirnx, cirny, cirnz, a, b;
	double x1, y1, x2, y2, x3, y3, x4, y4;
	for ( int i=0; i < 2; ++i ) {
		// conver from local to global
		transform3DCoordLocal2Global( px0, py0, pz0, center[i].x, center[i].y, center[i].z, nx0, ny0, outx, outy, outz );
		transform3DCoordGlobal2Local( x, y, z, outx, outy, outz, nx, ny, loc_x, loc_y, loc_z );
		// now we have 3D circle  at (loc_x, loc_y, loc_z) and direction (circlenx, circleny)

		project3DCircleToXYPlane( loc_x, loc_y, loc_z, pr0, circlenx, circleny, cirx, ciry, a, b, cirnx );
		// we have an ellipse on xy-plane in normal cube space   (cirx, ciry) (a,b) cirnx
		boundingBoxOfRotatedEllipse( cirx, ciry, a, b, cirnx, x1, y1, x2, y2, x3, y3, x4, y4 );
		if ( ! locIn2DCenterBox( x1, y1, w, d, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x2, y2, w, d, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x3, y3, w, d, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x4, y4, w, d, strict ) ) { return false; }

		project3DCircleToYZPlane( loc_x, loc_y, loc_z, pr0, circleny, circlenz, ciry, cirz, a, b, cirny );
		// we have an ellipse on xy-plane in normal cube space   (ciry, cirz) (a,b) cirny
		boundingBoxOfRotatedEllipse( ciry, cirz, a, b, cirny, x1, y1, x2, y2, x3, y3, x4, y4 );
		if ( ! locIn2DCenterBox( x1, y1, d, h, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x2, y2, d, h, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x3, y3, d, h, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x4, y4, d, h, strict ) ) { return false; }

		project3DCircleToZXPlane( loc_x, loc_y, loc_z, pr0, circlenz, circlenx, cirz, cirx, a, b, cirnz );
		// we have an ellipse on xy-plane in normal cube space   (cirz, cirx) (a,b) cirnz
		boundingBoxOfRotatedEllipse( cirz, cirx, a, b, cirnz, x1, y1, x2, y2, x3, y3, x4, y4 );
		if ( ! locIn2DCenterBox( x1, y1, h, w, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x2, y2, h, w, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x3, y3, h, w, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x4, y4, h, w, strict ) ) { return false; }

    }

	return true;

	#else

	JagPoint3D center[2];
	center[0].x = 0.0; center[0].y = 0.0; center[0].z = -c0;
	center[1].x = 0.0; center[1].y = 0.0; center[1].z = c0;
	double gx,gy,gz;
	for ( int i=0; i < 2; ++i ) {
		JagVector<JagPoint3D> vec;
		samplesOn3DCircle( center[i].x, center[i].y, center[i].z, pr0, nx0, ny0, NUM_SAMPLE, vec );
		for ( int j=0; j < vec.size(); ++j ) {
			transform3DCoordLocal2Global( px0, py0, pz0, vec[j].x, vec[j].y, vec[j].z, nx0, ny0, gx, gy, gz );
			gx -= x; gy -= y; gz -= z;
			if ( ! locIn3DCenterBox( gx,gy,gz, w,d,h, strict ) ) return false;
		}
	}
	return true;

	#endif
}

bool JagGeo::cylinderWithinSphere(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
                                double x, double y, double z, double r, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, pr0,pr0,c0, x, y, z, r,r,r ) ) { return false; }

	JagPoint3D center[2];
	center[0].x = 0.0; center[0].y = 0.0; center[0].z = -c0;
	center[1].x = 0.0; center[1].y = 0.0; center[1].z = c0;
	double gx,gy,gz;
	for ( int i=0; i < 2; ++i ) {
		JagVector<JagPoint3D> vec;
		samplesOn3DCircle( center[i].x, center[i].y, center[i].z, pr0, nx0, ny0, NUM_SAMPLE, vec );
		for ( int j=0; j < vec.size(); ++j ) {
			transform3DCoordLocal2Global( px0, py0, pz0, vec[j].x, vec[j].y, vec[j].z, nx0, ny0, gx, gy, gz );
			if ( ! point3DWithinSphere( gx,gy,gz, x, y, z, r, strict ) ) return false;
		}
	}
	return true;
}

bool JagGeo::cylinderWithinEllipsoid(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
						        	double x, double y, double z, 
							    	double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, pr0,c0,c0, x, y, z, w,d,h ) ) { return false; }

	JagPoint3D center[2];
	center[0].x = 0.0; center[0].y = 0.0; center[0].z = -c0;
	center[1].x = 0.0; center[1].y = 0.0; center[1].z = c0;
	double gx,gy,gz;
	for ( int i=0; i < 2; ++i ) {
		JagVector<JagPoint3D> vec;
		samplesOn3DCircle( center[i].x, center[i].y, center[i].z, pr0, nx0, ny0, NUM_SAMPLE, vec );
		for ( int j=0; j < vec.size(); ++j ) {
			transform3DCoordLocal2Global( px0, py0, pz0, vec[j].x, vec[j].y, vec[j].z, nx0, ny0, gx, gy, gz );
			if ( ! point3DWithinEllipsoid( gx,gy,gz, x, y, z, w,d,h, nx, ny, strict ) ) return false;
		}
	}
	return true;
}

bool JagGeo::cylinderWithinCone(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
						        	double x0, double y0, double z0, 
							    	double r, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, pr0,pr0,c0, x0, y0, z0, r,r,h ) ) { return false; }

	JagPoint3D center[2];
	center[0].x = 0.0; center[0].y = 0.0; center[0].z = -c0;
	center[1].x = 0.0; center[1].y = 0.0; center[1].z = c0;
	double gx,gy,gz;
	for ( int i=0; i < 2; ++i ) {
		JagVector<JagPoint3D> vec;
		samplesOn3DCircle( center[i].x, center[i].y, center[i].z, pr0, nx0, ny0, NUM_SAMPLE, vec );
		for ( int j=0; j < vec.size(); ++j ) {
			transform3DCoordLocal2Global( px0, py0, pz0, vec[j].x, vec[j].y, vec[j].z, nx0, ny0, gx, gy, gz );
			if ( ! point3DWithinCone( gx,gy,gz, x0, y0, z0, r, h, nx, ny, strict ) ) return false;
		}
	}
	return true;
}


// faster than coneWithinCube()
bool JagGeo::coneWithinCube_test(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
						        double x, double y, double z, double r, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;

	#if 0
	JagPoint3D center[2];
	// upper and lower local circles of the cynliner
	center[0].x = 0.0; center[0].y = 0.0; center[0].z = c0; 
	center[1].x = 0.0; center[1].y = 0.0; center[1].z = -c0; 

	double circlenx, circleny, circlenz; // direction of nx0 ny0 in nx ny
	transform3DDirection( nx0, ny0, nx, ny, circlenx, circleny );
	circlenz = sqrt( 1.0 - circlenx*circlenx - circleny*circleny );
	double outx, outy, outz, loc_x, loc_y, loc_z, cirx, ciry, cirz, cirnx, cirny, cirnz, a, b;
	double x1, y1, x2, y2, x3, y3, x4, y4;
	// for does only 1 point
	for ( int i=0; i < 1; ++i ) {
		// conver from local to global
		transform3DCoordLocal2Global( px0, py0, pz0, center[i].x, center[i].y, center[i].z, nx0, ny0, outx, outy, outz );
		transform3DCoordGlobal2Local( x, y, z, outx, outy, outz, nx, ny, loc_x, loc_y, loc_z );
		// now we have 3D circle  at (loc_x, loc_y, loc_z) and direction (circlenx, circleny)

		project3DCircleToXYPlane( loc_x, loc_y, loc_z, pr0, circlenx, circleny, cirx, ciry, a, b, cirnx );
		// we have an ellipse on xy-plane in normal cube space   (cirx, ciry) (a,b) cirnx
		boundingBoxOfRotatedEllipse( cirx, ciry, a, b, cirnx, x1, y1, x2, y2, x3, y3, x4, y4 );
		if ( ! locIn2DCenterBox( x1, y1, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x2, y2, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x3, y3, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x4, y4, r, r, strict ) ) { return false; }

		project3DCircleToYZPlane( loc_x, loc_y, loc_z, pr0, circleny, circlenz, ciry, cirz, a, b, cirny );
		// we have an ellipse on xy-plane in normal cube space   (ciry, cirz) (a,b) cirny
		boundingBoxOfRotatedEllipse( ciry, cirz, a, b, cirny, x1, y1, x2, y2, x3, y3, x4, y4 );
		if ( ! locIn2DCenterBox( x1, y1, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x2, y2, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x3, y3, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x4, y4, r, r, strict ) ) { return false; }

		project3DCircleToZXPlane( loc_x, loc_y, loc_z, pr0, circlenz, circlenx, cirz, cirx, a, b, cirnz );
		// we have an ellipse on xy-plane in normal cube space   (cirz, cirx) (a,b) cirnz
		boundingBoxOfRotatedEllipse( cirz, cirx, a, b, cirnz, x1, y1, x2, y2, x3, y3, x4, y4 );
		if ( ! locIn2DCenterBox( x1, y1, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x2, y2, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x3, y3, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x4, y4, r, r, strict ) ) { return false; }

    }

	// the tip
	transform3DCoordLocal2Global( px0, py0, pz0, center[1].x, center[1].y, center[1].z, nx0, ny0, outx, outy, outz );
	transform3DCoordGlobal2Local( x, y, z, outx, outy, outz, nx, ny, loc_x, loc_y, loc_z );
	if ( ! locIn3DCenterBox( loc_x, loc_y, loc_z, r, r, r, strict ) ) { return false; }

	return true;
	#else

	double gx,gy,gz, locx, locy, locz;
	// base points
	JagVector<JagPoint3D> vec;
	samplesOn3DCircle( 0.0, 0.0, -c0, pr0, nx0, ny0, NUM_SAMPLE, vec );
	for ( int j=0; j < vec.size(); ++j ) {
		transform3DCoordLocal2Global( px0, py0, pz0, vec[j].x, vec[j].y, vec[j].z, nx0, ny0, gx, gy, gz );
		transform3DCoordGlobal2Local( x,y,z, gx, gy, gz, nx, ny, locx, locy, locz );
		if ( ! locIn3DCenterBox( locx, locy, locz, r, r, r, strict ) ) { return false; }
	}
	// tip
	transform3DCoordLocal2Global( px0, py0, pz0, 0.0, 0.0, c0, nx0, ny0, gx, gy, gz );
	transform3DCoordGlobal2Local( x,y,z, gx, gy, gz, nx, ny, locx, locy, locz );
	if ( ! locIn3DCenterBox( locx, locy, locz, r, r, r, strict ) ) { return false; }
	return true;
	#endif

}

///////////////////////////////////// 3D cone /////////////////////////////////
bool JagGeo::coneWithinCube(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
						        double x, double y, double z, double r, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, pr0,pr0,c0, x, y, z, r,r,r ) ) { return false; }

	#if 1
	JagPoint3D center[2];
	// upper and lower local circles of the cynliner
	center[0].x = 0.0; center[0].y = 0.0; center[0].z = c0; 
	center[1].x = 0.0; center[1].y = 0.0; center[1].z = -c0; 

	double circlenx, circleny, circlenz; // direction of nx0 ny0 in nx ny
	transform3DDirection( nx0, ny0, nx, ny, circlenx, circleny );
	circlenz = sqrt( 1.0 - circlenx*circlenx - circleny*circleny );
	double outx, outy, outz, loc_x, loc_y, loc_z, cirx, ciry, cirz, cirnx, cirny, cirnz, a, b;
	double x1, y1, x2, y2, x3, y3, x4, y4;
	// for does only 1 point
	for ( int i=0; i < 1; ++i ) {
		// conver from local to global
		transform3DCoordLocal2Global( px0, py0, pz0, center[i].x, center[i].y, center[i].z, nx0, ny0, outx, outy, outz );
		transform3DCoordGlobal2Local( x, y, z, outx, outy, outz, nx, ny, loc_x, loc_y, loc_z );
		// now we have 3D circle  at (loc_x, loc_y, loc_z) and direction (circlenx, circleny)

		project3DCircleToXYPlane( loc_x, loc_y, loc_z, pr0, circlenx, circleny, cirx, ciry, a, b, cirnx );
		// we have an ellipse on xy-plane in normal cube space   (cirx, ciry) (a,b) cirnx
		boundingBoxOfRotatedEllipse( cirx, ciry, a, b, cirnx, x1, y1, x2, y2, x3, y3, x4, y4 );
		if ( ! locIn2DCenterBox( x1, y1, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x2, y2, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x3, y3, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x4, y4, r, r, strict ) ) { return false; }

		project3DCircleToYZPlane( loc_x, loc_y, loc_z, pr0, circleny, circlenz, ciry, cirz, a, b, cirny );
		// we have an ellipse on xy-plane in normal cube space   (ciry, cirz) (a,b) cirny
		boundingBoxOfRotatedEllipse( ciry, cirz, a, b, cirny, x1, y1, x2, y2, x3, y3, x4, y4 );
		if ( ! locIn2DCenterBox( x1, y1, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x2, y2, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x3, y3, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x4, y4, r, r, strict ) ) { return false; }

		project3DCircleToZXPlane( loc_x, loc_y, loc_z, pr0, circlenz, circlenx, cirz, cirx, a, b, cirnz );
		// we have an ellipse on xy-plane in normal cube space   (cirz, cirx) (a,b) cirnz
		boundingBoxOfRotatedEllipse( cirz, cirx, a, b, cirnz, x1, y1, x2, y2, x3, y3, x4, y4 );
		if ( ! locIn2DCenterBox( x1, y1, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x2, y2, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x3, y3, r, r, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x4, y4, r, r, strict ) ) { return false; }

    }

	// the tip
	transform3DCoordLocal2Global( px0, py0, pz0, center[1].x, center[1].y, center[1].z, nx0, ny0, outx, outy, outz );
	transform3DCoordGlobal2Local( x, y, z, outx, outy, outz, nx, ny, loc_x, loc_y, loc_z );
	if ( ! locIn3DCenterBox( loc_x, loc_y, loc_z, r, r, r, strict ) ) { return false; }
	return true;

	#else

	double gx,gy,gz, locx, locy, locz;
	// base points
	JagVector<JagPoint3D> vec;
	samplesOn3DCircle( 0.0, 0.0, -c0, pr0, nx0, ny0, NUM_SAMPLE, vec );
	for ( int j=0; j < vec.size(); ++j ) {
		transform3DCoordLocal2Global( px0, py0, pz0, vec[j].x, vec[j].y, vec[j].z, nx0, ny0, gx, gy, gz );
		transform3DCoordGlobal2Local( x,y,z, gx, gy, gz, nx, ny, locx, locy, locz );
		if ( ! locIn3DCenterBox( locx, locy, locz, r, r, r, strict ) ) { return false; }
	}
	// tip
	transform3DCoordLocal2Global( px0, py0, pz0, 0.0, 0.0, c0, nx0, ny0, gx, gy, gz );
	transform3DCoordGlobal2Local( x,y,z, gx, gy, gz, nx, ny, locx, locy, locz );
	if ( ! locIn3DCenterBox( locx, locy, locz, r, r, r, strict ) ) { return false; }
	return true;
	#endif

}


bool JagGeo::coneWithinBox(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
						        double x, double y, double z, 
							    double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, pr0,pr0,c0, x, y, z, w,d,h ) ) { return false; }

	#if 1
	JagPoint3D center[2];
	// upper and lower local circles of the cynliner
	center[0].x = 0.0; center[0].y = 0.0; center[0].z = c0; 
	center[1].x = 0.0; center[1].y = 0.0; center[1].z = -c0; 

	double circlenx, circleny, circlenz; // direction of nx0 ny0 in nx ny
	transform3DDirection( nx0, ny0, nx, ny, circlenx, circleny );
	circlenz = sqrt( 1.0 - circlenx*circlenx - circleny*circleny );
	double outx, outy, outz, loc_x, loc_y, loc_z, cirx, ciry, cirz, cirnx, cirny, cirnz, a, b;
	double x1, y1, x2, y2, x3, y3, x4, y4;
	for ( int i=0; i < 1; ++i ) {
		// conver from local to global
		transform3DCoordLocal2Global( px0, py0, pz0, center[i].x, center[i].y, center[i].z, nx0, ny0, outx, outy, outz );
		transform3DCoordGlobal2Local( x, y, z, outx, outy, outz, nx, ny, loc_x, loc_y, loc_z );
		// now we have 3D circle  at (loc_x, loc_y, loc_z) and direction (circlenx, circleny)

		project3DCircleToXYPlane( loc_x, loc_y, loc_z, pr0, circlenx, circleny, cirx, ciry, a, b, cirnx );
		// we have an ellipse on xy-plane in normal cube space   (cirx, ciry) (a,b) cirnx
		boundingBoxOfRotatedEllipse( cirx, ciry, a, b, cirnx, x1, y1, x2, y2, x3, y3, x4, y4 );
		if ( ! locIn2DCenterBox( x1, y1, w, d, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x2, y2, w, d, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x3, y3, w, d, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x4, y4, w, d, strict ) ) { return false; }

		project3DCircleToYZPlane( loc_x, loc_y, loc_z, pr0, circleny, circlenz, ciry, cirz, a, b, cirny );
		// we have an ellipse on xy-plane in normal cube space   (ciry, cirz) (a,b) cirny
		boundingBoxOfRotatedEllipse( ciry, cirz, a, b, cirny, x1, y1, x2, y2, x3, y3, x4, y4 );
		if ( ! locIn2DCenterBox( x1, y1, d, h, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x2, y2, d, h, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x3, y3, d, h, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x4, y4, d, h, strict ) ) { return false; }

		project3DCircleToZXPlane( loc_x, loc_y, loc_z, pr0, circlenz, circlenx, cirz, cirx, a, b, cirnz );
		// we have an ellipse on xy-plane in normal cube space   (cirz, cirx) (a,b) cirnz
		boundingBoxOfRotatedEllipse( cirz, cirx, a, b, cirnz, x1, y1, x2, y2, x3, y3, x4, y4 );
		if ( ! locIn2DCenterBox( x1, y1, h, w, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x2, y2, h, w, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x3, y3, h, w, strict ) ) { return false; }
		if ( ! locIn2DCenterBox( x4, y4, h, w, strict ) ) { return false; }
    }

	// the tip
	transform3DCoordLocal2Global( px0, py0, pz0, center[1].x, center[1].y, center[1].z, nx0, ny0, outx, outy, outz );
	transform3DCoordGlobal2Local( x, y, z, outx, outy, outz, nx, ny, loc_x, loc_y, loc_z );
	if ( ! locIn3DCenterBox( loc_x, loc_y, loc_z, w, d, h, strict ) ) { return false; }
	return true;

	#else

	double gx,gy,gz, locx, locy, locz;
	// base points
	JagVector<JagPoint3D> vec;
	samplesOn3DCircle( 0.0, 0.0, -c0, pr0, nx0, ny0, NUM_SAMPLE, vec );
	for ( int j=0; j < vec.size(); ++j ) {
		transform3DCoordLocal2Global( px0, py0, pz0, vec[j].x, vec[j].y, vec[j].z, nx0, ny0, gx, gy, gz );
		transform3DCoordGlobal2Local( x,y,z, gx, gy, gz, nx, ny, locx, locy, locz );
		if ( ! locIn3DCenterBox( locx, locy, locz, w, d, h, strict ) ) { return false; }
	}
	// tip
	transform3DCoordLocal2Global( px0, py0, pz0, 0.0, 0.0, c0, nx0, ny0, gx, gy, gz );
	transform3DCoordGlobal2Local( x,y,z, gx, gy, gz, nx, ny, locx, locy, locz );
	if ( ! locIn3DCenterBox( locx, locy, locz, w, d, h, strict ) ) { return false; }
	return true;
	#endif

}

bool JagGeo::coneWithinSphere(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
                                double x, double y, double z, double r, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, pr0,pr0,c0, x, y, z, r,r,r ) ) { return false; }

	double gx,gy,gz;
	JagVector<JagPoint3D> vec;
	samplesOn3DCircle( 0.0, 0.0, -c0, pr0, nx0, ny0, NUM_SAMPLE, vec );
	for ( int j=0; j < vec.size(); ++j ) {
		transform3DCoordLocal2Global( px0, py0, pz0, vec[j].x, vec[j].y, vec[j].z, nx0, ny0, gx, gy, gz );
		if ( ! point3DWithinSphere( gx,gy,gz, x,y,z,r,strict ) ) { return false;}
	}
	// tip
	transform3DCoordLocal2Global( px0, py0, pz0, 0.0, 0.0, c0, nx0, ny0, gx, gy, gz );
	if ( ! point3DWithinSphere( gx,gy,gz, x,y,z,r,strict ) ) { return false;}
	return true;
}

bool JagGeo::coneWithinEllipsoid(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
						        	double x, double y, double z, 
							    	double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, pr0,c0,c0, x, y, z, w,d,h ) ) { return false; }

	double gx,gy,gz;
	JagVector<JagPoint3D> vec;
	samplesOn3DCircle( 0.0, 0.0, -c0, pr0, nx0, ny0, NUM_SAMPLE, vec );
	for ( int j=0; j < vec.size(); ++j ) {
		transform3DCoordLocal2Global( px0, py0, pz0, vec[j].x, vec[j].y, vec[j].z, nx0, ny0, gx, gy, gz );
		if ( ! point3DWithinEllipsoid( gx,gy,gz, x,y,z,w,d,h,nx,ny,strict ) ) { return false;}
	}
	// tip
	transform3DCoordLocal2Global( px0, py0, pz0, 0.0, 0.0, c0, nx0, ny0, gx, gy, gz );
	if ( ! point3DWithinEllipsoid( gx,gy,gz, x,y,z,w,d,h,nx,ny,strict ) ) { return false;}
	return true;
}


bool JagGeo::coneWithinCone(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
						        	double x, double y, double z, 
							    	double r, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, pr0,pr0,c0, x, y, z, r,r,h ) ) { return false; }

	double gx,gy,gz;
	JagVector<JagPoint3D> vec;
	samplesOn3DCircle( 0.0, 0.0, -c0, pr0, nx0, ny0, NUM_SAMPLE, vec );
	for ( int j=0; j < vec.size(); ++j ) {
		transform3DCoordLocal2Global( px0, py0, pz0, vec[j].x, vec[j].y, vec[j].z, nx0, ny0, gx, gy, gz );
		if ( ! point3DWithinCone( gx,gy,gz, x,y,z,r,h,nx,ny,strict ) ) { return false;}
	}
	// tip
	transform3DCoordLocal2Global( px0, py0, pz0, 0.0, 0.0, c0, nx0, ny0, gx, gy, gz );
	if ( ! point3DWithinCone( gx,gy,gz, x,y,z,r,h,nx,ny,strict ) ) { return false;}
	return true;
}


bool JagGeo::circle3DWithinCone( double px0, double py0, double pz0, double pr0, double nx0, double ny0,
                                 double x0, double y0, double z0,
                                 double r, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, pr0,pr0,pr0, x0, y0, z0, r,r,h ) ) { return false; }

	JagVector<JagPoint3D> vec;
	samplesOn3DCircle( px0, py0, pz0, pr0, nx0, ny0, NUM_SAMPLE, vec );
	double sq_x, sq_y, sq_z;
	for ( int i=0; i < vec.size(); ++i ) {
		transform3DCoordLocal2Global( px0, py0, pz0, vec[i].x, vec[i].y, vec[i].z, nx0, ny0, sq_x, sq_y, sq_z );
		if ( ! point3DWithinCone( sq_x, sq_y, sq_z, x0, y0, z0, r, h, nx, ny, strict ) ) return false;
	}
	return true;
}

bool JagGeo::sphereWithinCone(  double px0, double py0, double pz0, double pr0,
						        	double x0, double y0, double z0, 
							    	double r, double h, double nx, double ny, bool strict )
{
	//if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, pr0,pr0,pr0, x0, y0, z0, r,r,h ) ) { return false; }

	if ( jagIsZero(r) && jagIsZero(h) ) return false;
    double locx, locy, locz;
    rotate3DCoordGlobal2Local( px0, py0, pz0, nx, ny, locx, locy, locz );
	double d = (h-locz)*r/sqrt(r*r+h*h );
	if ( strict ) {
		if ( pr0 < d ) { return true; }
	} else {
		if ( jagLE(pr0, d) ) return true;
	}
	return false;
}

bool JagGeo::point3DWithinPoint3D( double px, double py, double pz,  double x1, double y1, double z1, bool strict )
{
	if ( strict ) return false;
	if ( jagEQ(px,x1) && jagEQ(py,y1) && jagEQ(pz,z1) ) {
		return true;
	}
	return false;

}

bool JagGeo::point3DWithinLine3D( double px, double py, double pz, double x1, double y1, double z1, 
								  double x2, double y2, double z2, bool strict )
{
	if ( ! pointWithinLine(px,py, x1,y1,x2,y2,strict ) ) return false;
	if ( ! pointWithinLine(py,pz, y1,z1,y2,z2,strict ) ) return false;
	if ( ! pointWithinLine(pz,px, z1,x1,z2,x2,strict ) ) return false;
	return true;
}

bool JagGeo::pointWithinNormalEllipse( double px, double py, double w, double h, bool strict )
{
	if ( jagIsZero(w) || jagIsZero(h) ) return false;

	if ( px < -w || px > w || py < -h || py > h ) return false;

	double f = px*px/(w*w) + py*py/(h*h);
	if ( strict ) {
		if ( f < 1.0 ) return true; 
	} else {
		if ( jagLE(f, 1.0) ) return true; 
	}
	return false;
}

bool JagGeo::point3DWithinNormalEllipsoid( double px, double py, double pz,
									double w, double d, double h, bool strict )
{
	if ( jagIsZero(w) || jagIsZero(d) || jagIsZero(h) ) return false;
	if ( px < -w || px > w || py < d || py > d || pz < h || pz > h) return false;

	double f = px*px/(w*w) + py*py/(d*d) + pz*pz/(h*h);
	if ( strict ) {
		if ( f < 1.0 ) return true; 
	} else {
		if ( jagLE(f, 1.0) ) return true; 
	}
	return false;

}

bool JagGeo::point3DWithinNormalCone( double px, double py, double pz, 
										 double r, double h, bool strict )
{
	//prt(("s2203 point3DWithinNormalCone px=%f py=%f pz=%f r=%f h=%f\n", px,py,pz,r,h ));
	if ( strict ) {
		// canot be on boundary
		if ( jagLE( pz, -h) || jagGE( pz, h ) ) return false;
	} else {
		if ( pz < -h || pz > h ) return false;
	}

	double rz = ( 1.0 - pz/h ) * r;
	if ( rz < 0.0 ) return false;

	double dist2 = px*px + py*py;
	if ( strict ) {
		if (  dist2 < rz*rz ) return true;
	} else {
		if ( jagLE(dist2, rz*rz) ) return true;
	}

	//prt(("s2550 dist2=%f rz*rz=%f\n", dist2,  rz*rz ));
	return false;
}

bool JagGeo::point3DWithinNormalCylinder( double px, double py, double pz, 
										 double r, double h, bool strict )
{
	if ( strict ) {
		// canot be on boundary
		if ( jagLE( pz, -h) || jagGE( pz, h ) ) return false;
	} else {
		if ( pz < -h || pz > h ) return false;
	}

	double dist2 = px*px + py*py;
	if ( strict ) {
		if (  dist2 < r*r ) return true;
	} else {
		if ( jagLE(dist2, r*r) ) return true;
	}

	return false;
}


bool JagGeo::point3DWithinCone( double px, double py, double pz, 
								double x0, double y0, double z0,
								 double r, double h,  double nx, double ny, bool strict )
{
	double locx, locy, locz;
	transform3DCoordGlobal2Local( x0,y0,z0, px,py,pz, nx, ny, locx, locy, locz );
	/***
	prt(("s2220 point3DWithinCone px=%f py=%f pz=%f locx=%f locy=%f locz=%f\n",
				px,py,pz, locx, locy, locz ));
	***/
	return point3DWithinNormalCone( locx, locy, locz, r, h, strict );
}

bool JagGeo::point3DWithinCylinder( double px, double py, double pz, 
								double x0, double y0, double z0,
								 double r, double h,  double nx, double ny, bool strict )
{
	double locx, locy, locz;
	transform3DCoordGlobal2Local( x0,y0,z0, px,py,pz, nx, ny, locx, locy, locz );
	return point3DWithinNormalCylinder( locx, locy, locz, r, h, strict );
}



//////////////////////////// end within methods ////////////////////////////////////////////////


/////////////////////////////// start intersect methods //////////////////////////////////////////////
bool JagGeo::doPointIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	if ( colType2 == JAG_C_COL_TYPE_POINT ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		return pointWithinPoint( px0, py0, x0, y0, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_LINE ) {
		double x1 = jagatof( sp2[0].c_str() ); 
		double y1 = jagatof( sp2[1].c_str() ); 
		double x2 = jagatof( sp2[2].c_str() ); 
		double y2 = jagatof( sp2[3].c_str() ); 
		return pointWithinLine( px0, py0, x1, y1, x2, y2, strict );
	} else if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
    	JagPoint2D point( sp1[0].c_str(), sp1[1].c_str() );
    	JagPoint2D p1( sp2[0].c_str(), sp2[1].c_str() );
    	JagPoint2D p2( sp2[2].c_str(), sp2[3].c_str() );
    	JagPoint2D p3( sp2[4].c_str(), sp2[5].c_str() );
		return pointWithinTriangle( point, p1, p2, p3, strict, true );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3);
		//return pointWithinSquare( px0, py0, x0, y0, r, nx, strict );
		return pointWithinRectangle( px0, py0, x0, y0, r,r, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
			double x0 = jagatof( sp2[0].c_str() ); 
			double y0 = jagatof( sp2[1].c_str() ); 
			double w = jagatof( sp2[2].c_str() ); 
			double h = jagatof( sp2[3].c_str() ); 
			double nx = safeget( sp2, 4 );
			return pointWithinRectangle( px0, py0, x0, y0, w,h, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
			double x = jagatof( sp2[0].c_str() ); 
			double y = jagatof( sp2[1].c_str() ); 
			double r = jagatof( sp2[2].c_str() );
			return pointWithinCircle( px0, py0, x, y, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
			double x0 = jagatof( sp2[0].c_str() ); 
			double y0 = jagatof( sp2[1].c_str() ); 
			double w = jagatof( sp2[2].c_str() );
			double h = jagatof( sp2[3].c_str() );
			double nx = safeget( sp2, 4 );
			double ny = safeget( sp2, 5 );
			return pointWithinEllipse( px0, py0, x0, y0, w, h, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
			return pointWithinPolygon( px0, py0, mk2, sp2, false );
	}
	return false;
}

bool JagGeo::doPoint3DIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	if ( colType2 == JAG_C_COL_TYPE_POINT3D ) {
		double x1 = jagatof( sp2[0].c_str() ); 
		double y1 = jagatof( sp2[1].c_str() ); 
		double z1 = jagatof( sp2[2].c_str() ); 
		return point3DWithinPoint3D( px0, py0, pz0, x1, y1, z1, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_LINE3D ) {
		double x1 = jagatof( sp2[0].c_str() ); 
		double y1 = jagatof( sp2[1].c_str() ); 
		double z1 = jagatof( sp2[2].c_str() ); 
		double x2 = jagatof( sp2[3].c_str() ); 
		double y2 = jagatof( sp2[4].c_str() ); 
		double z2 = jagatof( sp2[5].c_str() ); 
		return point3DWithinLine3D( px0, py0, pz0, x1, y1, z1, x2, y2, z2, strict );
	} else if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return point3DWithinBox( px0, py0, pz0, x0, y0, z0, r,r,r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double a = jagatof( sp2[3].c_str() ); 
		double b = jagatof( sp2[4].c_str() ); 
		double c = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return point3DWithinBox( px0, py0, pz0, x0, y0, z0, a,b,c, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return point3DWithinSphere( px0,py0,pz0, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double a = jagatof( sp2[3].c_str() );
		double b = jagatof( sp2[4].c_str() );
		double c = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return point3DWithinEllipsoid( px0, py0, pz0, x0, y0, z0, a,b,c, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return point3DWithinCone( px0,py0,pz0, x, y, z, r, h, nx, ny, strict );
	}
	return false;
}

bool JagGeo::doCircleIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pr0 = jagatof( sp1[2].c_str() ); 

	if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() );
		return circleIntersectCircle(px0,py0,pr0, x,y,r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() );
		double nx = safeget( sp2, 3);
		return circleIntersectRectangle(px0,py0,pr0, x,y,r,r,nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double w = jagatof( sp2[2].c_str() );
		double h = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4);
		return circleIntersectRectangle( px0, py0, pr0, x0, y0, w, h, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		double x1 = jagatof( sp2[0].c_str() ); 
		double y1 = jagatof( sp2[1].c_str() ); 
		double x2 = jagatof( sp2[2].c_str() ); 
		double y2 = jagatof( sp2[3].c_str() ); 
		double x3 = jagatof( sp2[4].c_str() ); 
		double y3 = jagatof( sp2[5].c_str() ); 
		return circleIntersectTriangle(px0, py0, pr0, x1, y1, x2, y2, x3, y3, strict, true );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4);
		return circleIntersectEllipse(px0, py0, pr0, x0, y0, a, b, nx, strict, true );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return circleIntersectPolygon(px0, py0, pr0, mk2, sp2, strict );
	}
	return false;
}

double JagGeo::doCircle3DArea( int srid1, const JagStrSplit &sp1 )
{
	double r = jagatof( sp1[3].c_str() ); 
	return r * r * JAG_PI;
}
// circle surface with x y z and orientation
bool JagGeo::doCircle3DIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double pr0 = jagatof( sp1[3].c_str() ); 

	double nx0 = 0.0;
	double ny0 = 0.0;
	if ( sp1.length() >= 5 ) { nx0 = jagatof( sp1[4].c_str() ); }
	if ( sp1.length() >= 6 ) { ny0 = jagatof( sp1[5].c_str() ); }

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return circle3DIntersectBox( px0, py0, pz0, pr0, nx0, ny0, x0, y0, z0, r,r,r, nx, ny, strict );
	} else if (  colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double a = jagatof( sp2[3].c_str() ); 
		double b = jagatof( sp2[4].c_str() ); 
		double c = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return circle3DIntersectBox( px0, py0, pz0, pr0, nx0, ny0, x0, y0, z0, a,b,c, nx, ny, strict );
	} else if (  colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return circle3DIntersectSphere( px0, py0, pz0, pr0, nx0, ny0, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return circle3DIntersectEllipsoid( px0, py0, pz0, pr0, nx0, ny0, x0, y0, z0, w,d,h,  nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5);
		double ny = safeget( sp2, 6);
		return circle3DIntersectCone( px0,py0,pz0,pr0,nx0,ny0, x, y, z, r, h, nx, ny, strict );
	}

	return false;
}

bool JagGeo::doSphereIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double pr0 = jagatof( sp1[2].c_str() ); 
	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4);
		double ny = safeget( sp2, 5);
		return ellipsoidIntersectBox( px0, py0, pz0, pr0,pr0,pr0,0.0,0.0, x0, y0, z0, r,r,r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 

		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return ellipsoidIntersectBox( px0, py0, pz0, pr0,pr0,pr0,0.0,0.0, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return ellipsoidIntersectEllipsoid( px0, py0, pz0, pr0,pr0,pr0,0.0,0.0, x, y, z, r,r,r, 0.0,0.0, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return ellipsoidIntersectEllipsoid( px0, py0, pz0, pr0,pr0,pr0,0.0,0.0, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5);
		double ny = safeget( sp2, 6);
		return ellipsoidIntersectCone( px0, py0, pz0, pr0,pr0,pr0,0.0,0.0,  x0, y0, z0, r,h, nx,ny, strict );
	}
	return false;
}

double JagGeo::doSquareArea( int srid1, const JagStrSplit &sp1 )
{
	double a = jagatof( sp1[2].c_str() ); 
	return (a*a*4.0);
}

double JagGeo::doSquare3DArea( int srid1, const JagStrSplit &sp1 )
{
	double a = jagatof( sp1[3].c_str() ); 
	return (a*a*4.0);
}

// 2D
bool JagGeo::doSquareIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	//prt(("s3033 doSquareIntersect colType2=[%s] \n", colType2.c_str() ));
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pr0 = jagatof( sp1[2].c_str() ); 
	double nx0 = safeget( sp1, 3 );

	// like point within
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return rectangleIntersectTriangle( px0, py0, pr0, pr0, nx0, x1, y1, x2, y2, x3, y3, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double nx = safeget(sp2, 3 );
		return rectangleIntersectRectangle( px0, py0, pr0,pr0, nx0, x0, y0, a,a, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		double nx = safeget(sp2, 4 );
		return rectangleIntersectRectangle( px0, py0, pr0,pr0, nx0, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		double nx = safeget(sp2, 3);
		return rectangleIntersectEllipse( px0, py0, pr0,pr0, nx0, x0, y0, r,r, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		double nx = safeget(sp2, 4);
		return rectangleIntersectEllipse( px0, py0, pr0,pr0, nx0, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return rectangleIntersectPolygon( px0, py0, pr0,pr0, nx0, mk2, sp2 );
	} else {
		throw 2341;
	}
	return false;
}

bool JagGeo::doSquare3DIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double pr0 = jagatof( sp1[3].c_str() ); 
	double nx0 = safeget( sp1, 4);
	double ny0 = safeget( sp1, 5);

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		// return square3DIntersectCube( px0, py0, pz0, pr0, nx0, ny0, x0, y0, z0, r, nx, ny, strict );
		return rectangle3DIntersectBox( px0, py0, pz0, pr0,pr0, nx0, ny0, x0, y0, z0, r,r,r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return rectangle3DIntersectBox( px0, py0, pz0, pr0,pr0,  nx0, ny0,x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return rectangle3DIntersectEllipsoid( px0, py0, pz0, pr0,pr0, nx0, ny0, x, y, z, r,r,r, 0.0,0.0, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return rectangle3DIntersectEllipsoid( px0, py0, pz0, pr0,pr0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return rectangle3DIntersectCone( px0, py0, pz0, pr0,pr0, nx0, ny0, x0, y0, z0, r,h, nx,ny, strict );
	}
	return false;
}


bool JagGeo::doCubeIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double pr0 = jagatof( sp1[3].c_str() ); 
	double nx0 = safeget(sp1, 4);
	double ny0 = safeget(sp1, 5);

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return boxIntersectBox( px0, py0, pz0, pr0,pr0,pr0, nx0, ny0, x0, y0, z0, r,r,r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return boxIntersectBox( px0, py0, pz0, pr0,pr0,pr0, nx0, ny0, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return boxIntersectEllipsoid( px0, py0, pz0, pr0,pr0,pr0, nx0, ny0, x, y, z, r,r,r, 0.0,0.0, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return boxIntersectEllipsoid( px0, py0, pz0, pr0,pr0,pr0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return boxIntersectCone( px0, py0, pz0, pr0,pr0,pr0, nx0, ny0, x0, y0, z0, r, h, nx,ny, strict );
	}
	return false;
}

// 2D
bool JagGeo::doRectangleIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double a0 = jagatof( sp1[2].c_str() ); 
	double b0 = jagatof( sp1[3].c_str() ); 
	double nx0 = safeget( sp1, 4 );

	// like point within
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return rectangleIntersectTriangle( px0, py0, a0, b0, nx0, x1, y1, x2, y2, x3, y3, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3 );
		return rectangleIntersectRectangle( px0, py0, a0, b0, nx0, x0, y0, a,a, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		return rectangleIntersectRectangle( px0, py0, a0, b0, nx0, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3 );
		return rectangleIntersectEllipse( px0, py0, a0, b0, nx0, x0, y0, r,r, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4 );
		return rectangleIntersectEllipse( px0, py0, a0, b0, nx0, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		return rectangleIntersectPolygon( px0, py0, a0, b0, nx0, mk2, sp2 );
	}
	return false;
}

// 3D rectiangle
bool JagGeo::doRectangle3DIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double a0 = jagatof( sp1[3].c_str() ); 
	double b0 = jagatof( sp1[4].c_str() ); 

	double nx0 = safeget( sp1, 5 );
	double ny0 = safeget( sp1, 6 );

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4);
		double ny = safeget( sp2, 5);
		return rectangle3DIntersectBox( px0, py0, pz0, a0, b0, nx0, ny0, x0, y0, z0, r,r,r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return rectangle3DIntersectBox( px0, py0, pz0, a0, b0, nx0, ny0, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return rectangle3DIntersectEllipsoid( px0, py0, pz0, a0, b0, nx0, ny0, x, y, z, r,r,r,0.0,0.0, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return rectangle3DIntersectEllipsoid( px0, py0, pz0, a0, b0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5);
		double ny = safeget( sp2, 6);
		return rectangle3DIntersectCone( px0, py0, pz0, a0, b0, nx0, ny0, x0, y0, z0, r,h, nx,ny, strict );
	}
	return false;
}

bool JagGeo::doBoxIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double a0 = jagatof( sp1[3].c_str() ); 
	double b0 = jagatof( sp1[4].c_str() ); 
	double c0 = jagatof( sp1[5].c_str() ); 
	double nx0 = safeget( sp1, 6 );
	double ny0 = safeget( sp1, 7 );

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return boxIntersectBox( px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, r,r,r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return boxIntersectBox( px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return boxIntersectEllipsoid( px0, py0, pz0, a0, b0, c0, nx0, ny0, x, y, z, r,r,r,0.0,0.0, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return boxIntersectEllipsoid( px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return boxIntersectCone( px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, r,h, nx,ny, strict );
	}
	return false;
}


// 3D
bool JagGeo::doCylinderIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	// not supported for now
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double pr0 = jagatof( sp1[3].c_str() ); 
	double c0 = jagatof( sp1[4].c_str() ); 

	double nx0 = safeget(sp1, 5);
	double ny0 = safeget(sp1, 6);

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return cylinderIntersectBox( px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, r,r,r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return cylinderIntersectBox( px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return cylinderIntersectEllipsoid( px0, py0, pz0, pr0, c0,  nx0, ny0, x, y, z, r,r,r,0.0,0.0, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return cylinderIntersectEllipsoid( px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return cylinderIntersectCone( px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, r,h, nx,ny, strict );
	}
	return false;
}

bool JagGeo::doConeIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double pr0 = jagatof( sp1[3].c_str() ); 
	double c0 = jagatof( sp1[4].c_str() ); 
	double nx0 = safeget(sp1, 5 );
	double ny0 = safeget(sp1, 6 );

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return coneIntersectBox( px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, r,r,r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return coneIntersectBox( px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return coneIntersectEllipsoid( px0, py0, pz0, pr0, c0,  nx0, ny0, x, y, z, r,r,r, 0.0,0.0, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return coneIntersectEllipsoid( px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return coneIntersectCone( px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, r,h, nx,ny, strict );
	} else {
		throw 2910;
	}
	return false;
}

// 2D
bool JagGeo::doEllipseIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double a0 = jagatof( sp1[2].c_str() ); 
	double b0 = jagatof( sp1[3].c_str() ); 
	double nx0 = safeget(sp1, 4);

	// like point within
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return ellipseIntersectTriangle( px0, py0, a0, b0, nx0, x1, y1, x2, y2, x3, y3, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3);
		return ellipseIntersectRectangle( px0, py0, a0, b0, nx0, x0, y0, a,a, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4);
		return ellipseIntersectRectangle( px0, py0, a0, b0, nx0, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3);
		return ellipseIntersectEllipse( px0, py0, a0, b0, nx0, x0, y0, r,r, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4);
		return ellipseIntersectEllipse( px0, py0, a0, b0, nx0, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return ellipseIntersectPolygon( px0, py0, a0, b0, nx0, mk2, sp2 );
	}
	return false;
}

// 3D ellipsoid
bool JagGeo::doEllipsoidIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double a0 = jagatof( sp1[3].c_str() ); 
	double b0 = jagatof( sp1[4].c_str() ); 
	double c0 = jagatof( sp1[5].c_str() ); 
	double nx0 = safeget( sp1, 6);
	double ny0 = safeget( sp1, 7);

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4);
		double ny = safeget( sp2, 5);
		return ellipsoidIntersectBox( px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, r,r,r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return ellipsoidIntersectBox( px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return ellipsoidIntersectEllipsoid( px0, py0, pz0, a0, b0, c0, nx0, ny0, x, y, z, r,r,r, 0.0,0.0, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return ellipsoidIntersectEllipsoid( px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5);
		double ny = safeget( sp2, 6);
		return ellipsoidIntersectCone( px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, r,h, nx,ny, strict );
	}
	return false;
}

// 2D triangle within
bool JagGeo::doTriangleIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double x10 = jagatof( sp1[0].c_str() );
	double y10 = jagatof( sp1[1].c_str() );
	double x20 = jagatof( sp1[2].c_str() );
	double y20 = jagatof( sp1[3].c_str() );
	double x30 = jagatof( sp1[4].c_str() );
	double y30 = jagatof( sp1[5].c_str() );

	// like point within
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return triangleIntersectTriangle( x10, y10, x20, y20, x30, y30, x1, y1, x2, y2, x3, y3, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_LINE ) {
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		return triangleIntersectLine( x10, y10, x20, y20, x30, y30, x1, y1, x2, y2 );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3);
		return triangleIntersectRectangle( x10, y10, x20, y20, x30, y30, x0, y0, a,a, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4);
		return triangleIntersectRectangle( x10, y10, x20, y20, x30, y30, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3);
		return triangleIntersectEllipse( x10, y10, x20, y20, x30, y30, x0, y0, r,r, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4);
		return triangleIntersectEllipse( x10, y10, x20, y20, x30, y30, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return triangleIntersectPolygon( x10, y10, x20, y20, x30, y30, mk2, sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING ) {
		return triangleIntersectLineString( x10, y10, x20, y20, x30, y30, mk2, sp2 );
	}
	return false;
}

// 3D  triangle
bool JagGeo::doTriangle3DIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double x10 = jagatof( sp1[0].c_str() );
	double y10 = jagatof( sp1[1].c_str() );
	double z10 = jagatof( sp1[2].c_str() );
	double x20 = jagatof( sp1[3].c_str() );
	double y20 = jagatof( sp1[4].c_str() );
	double z20 = jagatof( sp1[5].c_str() );
	double x30 = jagatof( sp1[6].c_str() );
	double y30 = jagatof( sp1[7].c_str() );
	double z30 = jagatof( sp1[8].c_str() );

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4);
		double ny = safeget( sp2, 5);
		return triangle3DIntersectBox( x10,y10,z10,x20,y20,z20,x30,y30,z30, x0, y0, z0, r,r,r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return triangle3DIntersectBox( x10,y10,z10,x20,y20,z20,x30,y30,z30, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return triangle3DIntersectEllipsoid( x10,y10,z10,x20,y20,z20,x30,y30,z30, x, y, z, r,r,r,0.0,0.0, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return triangle3DIntersectEllipsoid( x10,y10,z10,x20,y20,z20,x30,y30,z30, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5);
		double ny = safeget( sp2, 6);
		return triangle3DIntersectCone( x10,y10,z10,x20,y20,z20,x30,y30,z30, x0, y0, z0, r,h, nx,ny, strict );
	}
	return false;
}

// 2D line
bool JagGeo::doLineIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double x10 = jagatof( sp1[0].c_str() );
	double y10 = jagatof( sp1[1].c_str() );
	double x20 = jagatof( sp1[2].c_str() );
	double y20 = jagatof( sp1[3].c_str() );

	// like point within
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return lineIntersectTriangle( x10, y10, x20, y20, x1, y1, x2, y2, x3, y3, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING ) {
		return lineIntersectLineString( x10, y10, x20, y20, mk2, sp2, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3 );
		return lineIntersectRectangle( x10, y10, x20, y20, x0, y0, a,a, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		return lineIntersectRectangle( x10, y10, x20, y20, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3 );
		return lineIntersectEllipse( x10, y10, x20, y20, x0, y0, r,r, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4 );
		return lineIntersectEllipse( x10, y10, x20, y20, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return lineIntersectPolygon( x10, y10, x20, y20, mk2, sp2 );
	}
	return false;
}

bool JagGeo::doLine3DIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
										 int srid2, const JagStrSplit &sp2, bool strict )
{
	double x10 = jagatof( sp1[0].c_str() );
	double y10 = jagatof( sp1[1].c_str() );
	double z10 = jagatof( sp1[2].c_str() );
	double x20 = jagatof( sp1[3].c_str() );
	double y20 = jagatof( sp1[4].c_str() );
	double z20 = jagatof( sp1[5].c_str() );

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return line3DIntersectBox( x10,y10,z10,x20,y20,z20, x0, y0, z0, r,r,r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return line3DIntersectBox( x10,y10,z10,x20,y20,z20, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return line3DIntersectSphere( x10,y10,z10,x20,y20,z20, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return line3DIntersectEllipsoid( x10,y10,z10,x20,y20,z20, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return line3DIntersectCone( x10,y10,z10,x20,y20,z20, x0, y0, z0, r,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CYLINDER ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return line3DIntersectCylinder( x10,y10,z10,x20,y20,z20, x0, y0, z0, r,r,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_TRIANGLE3D ) {
		double x1 = jagatof( sp2[0].c_str() ); 
		double y1 = jagatof( sp2[1].c_str() ); 
		double z1 = jagatof( sp2[2].c_str() ); 
		double x2 = jagatof( sp2[3].c_str() ); 
		double y2 = jagatof( sp2[4].c_str() ); 
		double z2 = jagatof( sp2[5].c_str() ); 
		double x3 = jagatof( sp2[6].c_str() ); 
		double y3 = jagatof( sp2[7].c_str() ); 
		double z3 = jagatof( sp2[8].c_str() ); 
		JagLine3D line(x10, y10, z10, x20, y20, z20 );
		JagPoint3D p1(x1,y1,z1);
		JagPoint3D p2(x2,y2,z2);
		JagPoint3D p3(x3,y3,z3);
		JagPoint3D atPoint;
		return line3DIntersectTriangle3D( line, p1, p2, p3, atPoint ); 
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE3D ) {
		JagLine3D line(x10, y10, z10, x20, y20, z20 );
    	double px0 = jagatof( sp2[0].c_str() ); 
    	double py0 = jagatof( sp2[1].c_str() ); 
    	double pz0 = jagatof( sp2[2].c_str() ); 
    	double w = jagatof( sp2[3].c_str() ); 
    	double h = jagatof( sp2[4].c_str() ); 
    	double nx = safeget( sp2, 5 );
    	double ny = safeget( sp2, 6 );
		return line3DIntersectRectangle3D( line, px0, py0, pz0, w, h, nx, ny );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE3D ) {
		JagLine3D line(x10, y10, z10, x20, y20, z20 );
    	double px0 = jagatof( sp2[0].c_str() ); 
    	double py0 = jagatof( sp2[1].c_str() ); 
    	double pz0 = jagatof( sp2[2].c_str() ); 
    	double w = jagatof( sp2[3].c_str() ); 
    	double nx = safeget( sp2, 5 );
    	double ny = safeget( sp2, 6 );
		return line3DIntersectRectangle3D( line, px0, py0, pz0, w, w, nx, ny );
	}
	return false;
}


//////////////// point
// strict true: point strictly inside (no boundary) in triangle.
//        false: point can be on boundary
// boundcheck:  do bounding box check or not

////////////////////////////////////// 2D circle /////////////////////////
bool JagGeo::circleIntersectTriangle( double centrex, double centrey, double radius, double v1x, double v1y, double v2x, double v2y, 
								double v3x, double v3y, bool strict, bool bound )
{
	if ( bound ) {
		if ( bound2DTriangleDisjoint( v1x,v1y,v2x,v2y,v3x,v3y,centrex,centrey,radius,radius ) ) return false;
	}

	// see http://www.phatcode.net/articles.php?id=459
    // TEST 1: Vertex within circle
    double cx = centrex - v1x;
    double cy = centrey - v1y;
    double radiusSqr = radius*radius;
    double c1sqr = cx*cx + cy*cy - radiusSqr;
    
    if ( jagLE(c1sqr, 0.0) ) {
      	return true;
	}
    
    cx = centrex - v2x;
    cy = centrey - v2y;
    double c2sqr = cx*cx + cy*cy - radiusSqr;
    if ( jagLE(c2sqr, 0.0) ) {
      	return true;
	}
    
    cx = centrex - v3x;
    cy = centrey - v3y;
    double c3sqr = cx*cx + cy*cy - radiusSqr;
    if ( jagLE(c3sqr, 0.0) ) {
      	return true;
	}
    
    // TEST 2: Circle centre within triangle
	if ( pointInTriangle( centrex, centrey, v1x, v1y, v2x, v2y, v3x, v3y, false, true ) ) {
		return true;
	}

    /***
    e1x = v2x - v1x
    e1y = v2y - v1y
    
    e2x = v3x - v2x
    e2y = v3y - v2y
    
    e3x = v1x - v3x
    e3y = v1y - v3y
    ***/
    
    // TEST 3: Circle intersects edge
	double len, k, ex, ey;
    cx = centrex - v1x;
    cy = centrex - v1y;
    ex = v2x - v1x;
    ex = v2y - v1y;
    k = cx*ex + cy*ey;
    
    if ( k > 0.0 ) {
      len = ex*ex + ey*ey;
      if ( k < len ) {
        if ( jagLE(c1sqr*len, k*k ) ) {
          return true;
		}
      }
    }
    
    // Second edge
    cx = centrex - v2x;
    cy = centrex - v2y;
    ex = v3x - v2x;
    ex = v3y - v2y;
    k = cx*ex + cy*ey;
    if ( k > 0.0 ) {
      len = ex*ex + ey*ey;
      if ( k < len ) {
        if ( jagLE(c2sqr*len, k*k ) ) {
          return true;
		}
      }
    }
    
    
    // Third edge
    cx = centrex - v2x;
    cy = centrex - v2y;
    ex = v1x - v3x;
    ex = v1y - v3y;
    k = cx*ex + cy*ey;
    if ( k > 0.0 ) {
      len = ex*ex + ey*ey;
      if ( k < len ) {
        if ( jagLE(c3sqr*len, k*k ) ) {
          return true;
		}
      }
    }

    // We're done, no intersection
    return false;
}


bool JagGeo::circleIntersectEllipse( double px0, double py0, double pr, 
							  double x, double y, double w, double h, double nx, 
							  bool strict, bool bound )
{
	if ( bound ) {
		if ( bound2DDisjoint( px0,py0,pr,pr,  x,y,w,h ) ) return false;
	}

	double px, py;
	transform2DCoordGlobal2Local( x, y, px0, py0, nx, px, py );

	JagVector<JagPoint2D> vec;
	samplesOn2DCircle( px0, py0, pr, 2*NUM_SAMPLE, vec );
	for ( int i=0; i < vec.size(); ++i ) {
		if ( pointWithinEllipse( vec[i].x, vec[i].y, x, y, w, h, nx, strict ) ) {
			return true;
		}
	}

	JagVector<JagPoint2D> vec2;
	samplesOn2DEllipse( x, y, w, h, nx, 2*NUM_SAMPLE, vec2 );
	for ( int i=0; i < vec2.size(); ++i ) {
		if ( pointWithinCircle( vec2[i].x, vec2[i].y, px0,py0,pr, false ) ) {
			return true;
		}
	}

	return false;
}


bool JagGeo::circleIntersectPolygon( double px0, double py0, double pr, 
							  const AbaxDataString &mk2, const JagStrSplit &sp2,
							  bool strict )
{
	double bbx, bby, rx, ry;
	getPolygonBound( mk2, sp2, bbx, bby, rx, ry );
	if ( bound2DDisjoint( px0,py0,pr,pr,  bbx, bby, rx, ry ) ) return false;

	JagPolygon pgon;
	int rc = JagParser::addPolygonData( pgon, sp2, false );
	if ( rc < 0 ) return false;

	JagVector<JagPoint2D> vec;
	samplesOn2DCircle( px0, py0, pr, 2*NUM_SAMPLE, vec );
	for ( int i=0; i < vec.size(); ++i ) {
		if ( pointWithinPolygon( vec[i].x, vec[i].y, pgon ) ) {
			return true;
		}
	}

	int jj;
	for ( int i=0; i < pgon.size(); ++i ) {
		const JagLineString3D &linestr = pgon.linestr[i];
		for ( int j=0; j < linestr.size(); ++j ) {
			jj = ( j + 1) % linestr.size();
			if ( lineIntersectEllipse( linestr.point[j].x, linestr.point[j].y, 
									   linestr.point[jj].x, linestr.point[jj].y,
									   px0,py0,pr,pr,0.0 , strict ) ) {
				return true;
			}
		}
	}

	return false;
}


/////////////////////////////////////// 2D circle ////////////////////////////////////////
bool JagGeo::circleIntersectCircle( double px, double py, double pr, double x, double y, double r, bool strict )
{
	if ( px+pr < x-r || px-pr > x+r || py+pr < y-r || py-pr > y+r ) return false;
	double dist2  = (px-x)*(px-x) + (py-y)*(py-y);
	if ( jagLE( dist2,  fabs(pr+r)*fabs(pr+r) ) ) return true;
	return false;
}

bool JagGeo::circleIntersectRectangle( double px0, double py0, double pr, double x0, double y0, 
									double w, double h, double nx,  bool strict )
{
	if ( ! validDirection(nx) ) return false;
	if ( bound2DDisjoint( px0, py0, pr, pr, x0, y0, w, h ) ) return false;

	// circle center inside square
	if ( pointWithinRectangle( px0, py0, x0, y0, w, h, nx, false ) ) return true;

	// convert px0 py0 to rectangle local coords
	double locx, locy;
	transform2DCoordGlobal2Local( x0, y0, px0, py0, nx, locx, locy );
	JagLine2D line[4];
	edgesOfRectangle( w, h, line );
	int rel;
	for ( int i=0; i < 4; ++i ) {
		rel = relationLineCircle( line[i].x1, line[i].y1, line[i].x2, line[i].y2, locx, locy, pr );
		if ( rel > 0 ) { return true; }
	}
	return false;
}


//////////////////////////////////////// 3D circle ///////////////////////////////////////
bool JagGeo::circle3DIntersectBox( double px0, double py0, double pz0, double pr0, double nx0, double ny0,
							    double x, double y, double z,  double w, double d, double h, 
								double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, pr0,pr0,pr0, x, y, z, w,d,h ) ) { return false; }

	if (  point3DWithinBox( px0, py0, pz0,   x,y,z, w,d,h,nx,ny, false ) ) {
		return true;
	}

	int num = 12;
	JagLine3D line[num];
	edgesOfBox( w, d, h, line );
	transform3DLinesLocal2Global( x, y, z, nx, ny, num, line );  // to x-y-z
	for ( int i=0; i<num; ++i ) {
		if ( line3DIntersectEllipse3D( line[i], px0, py0, pz0, pr0,pr0, nx0, ny0 )  ) {
			return true;
		}
	}
	return false;
}

bool JagGeo::circle3DIntersectSphere( double px0, double py0, double pz0, double pr0, double nx0, double ny0,
								   double x, double y, double z, double r, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, pr0,pr0,pr0, x, y, z, r,r,r ) ) { return false; }

	double d2 = (px0-x)*(px0-x) + (py0-y)*(py0-y) + (pz0-z)*(pz0-z);
	if ( jagLE( d2, (pr0+r)*(pr0+r) ) ) return true;
	return false;
}


bool JagGeo::circle3DIntersectEllipsoid( double px0, double py0, double pz0, double pr0, double nx0, double ny0,
									  double x, double y, double z, double w, double d, double h,
									  double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if (  bound3DDisjoint( px0, py0, pz0, pr0,pr0,pr0, x, y, z, w,d,h ) ) { return false; }

	JagVector<JagPoint3D> vec;
	samplesOn3DCircle( px0, py0, pz0, pr0, nx0, ny0, NUM_SAMPLE, vec );
	double sq_x, sq_y, sq_z, locx, locy, locz ;
	for ( int i=0; i < vec.size(); ++i ) {
		transform3DCoordLocal2Global( px0, py0, pz0, vec[i].x, vec[i].y, vec[i].z, nx0, ny0, sq_x, sq_y, sq_z );
		transform3DCoordGlobal2Local( x, y, z, sq_x, sq_y, sq_z, nx, ny, locx, locy, locz );
		if ( point3DWithinNormalEllipsoid( locx, locy, locz, w,d,h, strict ) ) { return true; }
	}

	return false;
}



//////////////////////////// 2D rectangle //////////////////////////////////////////////////
bool JagGeo::rectangleIntersectTriangle( double px0, double py0, double a0, double b0, double nx0,
									double x1, double y1, double x2, double y2, double x3, double y3,
									bool strict )
{
	if ( ! validDirection(nx0) ) return false;
	if ( bound2DTriangleDisjoint(x1,y1,x2,y2,x3,y3, px0,py0,a0,b0 ) ) return false;

	int num = 4;
	JagLine2D line[num];
	edgesOfRectangle( a0, b0, line );
	transform2DLinesLocal2Global( px0, py0, nx0, num, line );
	for ( int i=0; i < num; ++i ) {
		if ( lineIntersectLine( line[i].x1, line[i].y1, line[i].x2, line[i].y2, x1, y1, x2, y2 ) ) return true;
		if ( lineIntersectLine( line[i].x1, line[i].y1, line[i].x2, line[i].y2, x2, y2, x3, y3 ) ) return true;
		if ( lineIntersectLine( line[i].x1, line[i].y1, line[i].x2, line[i].y2, x3, y3, x1, y1 ) ) return true;
	}
	return false;
}

bool JagGeo::rectangleIntersectRectangle( double px0, double py0, double a0, double b0, double nx0,
                                	double x0, double y0, double a, double b, double nx, bool strict )
{
	if ( ! validDirection(nx0) ) return false;
	if ( ! validDirection(nx) ) return false;
	if (  bound2DDisjoint( px0, py0, a0,b0, x0, y0, a, b ) ) { return false; }

	int num = 4;
	JagLine2D line1[num];
	edgesOfRectangle( a0, b0, line1 );
	transform2DLinesLocal2Global( px0, py0, nx0, num, line1 );

	JagLine2D line2[num];
	edgesOfRectangle( a, b, line2 );
	transform2DLinesLocal2Global( x0, y0, nx, num, line2 );
	for ( int i=0; i < num; ++i ) {
		for ( int j=0; j < num; ++j ) {
			if ( lineIntersectLine( line1[i].x1, line1[i].y1, line1[i].x2, line1[i].y2,
							        line1[j].x1, line1[j].y1, line1[j].x2, line1[j].y2 ) ) {
				return true;
			}
		}
	}
	return false;

}

bool JagGeo::rectangleIntersectEllipse( double px0, double py0, double a0, double b0, double nx0,
                                	double x0, double y0, double a, double b, double nx, bool strict )
{
	if ( ! validDirection(nx0) ) return false;
	if ( ! validDirection(nx) ) return false;
	if ( jagIsZero(a) || jagIsZero(b) ) return false;
	if (  bound2DDisjoint( px0, py0, a0,b0, x0, y0, a, b ) ) { return false; }

	if ( pointWithinRectangle( x0, y0, px0, py0, a0,b0, nx0, false ) ) { return true; }
	if ( pointWithinEllipse( px0, py0, x0, y0, a,b, nx, false ) ) { return true; }

	int num = 4;
	JagLine2D line[num];
	edgesOfRectangle( a0, b0, line );
	transform2DLinesLocal2Global( px0, py0, nx0, num, line );
	int rel;
	for (int i=0; i<num; ++i ) {
		rel = relationLineEllipse( line[i].x1, line[i].y1, line[i].x2, line[i].y2, x0, y0, a, b, nx );
		if ( rel > 0) { return true; }
	}
	return false;
}


bool JagGeo::rectangleIntersectPolygon( double px0, double py0, double a0, double b0, double nx0,
							  const AbaxDataString &mk2, const JagStrSplit &sp2 )
{
	double bbx, bby, rx, ry;
	getPolygonBound( mk2, sp2, bbx, bby, rx, ry );
	if ( bound2DDisjoint( px0,py0,a0,b0, bbx, bby, rx, ry ) ) return false;

	JagPolygon pgon;
	int rc = JagParser::addPolygonData( pgon, sp2, false );
	if ( rc < 0 ) return false;

	
	JagPoint2D corn[4];
	cornersOfRectangle( a0, b0, corn );
	transform2DEdgesLocal2Global( px0, py0, nx0, 4, corn );
	for ( int i=0; i < 4; ++i ) {
		if ( pointWithinPolygon( corn[i].x, corn[i].y, pgon ) ) {
			return true;
		}
	}

	int jj;
	for ( int i=0; i < pgon.size(); ++i ) {
		const JagLineString3D &linestr = pgon.linestr[i];
		for ( int j=0; j < linestr.size(); ++j ) {
			jj = ( j + 1) % linestr.size();
			if ( lineIntersectRectangle( linestr.point[j].x, linestr.point[j].y, 
									   linestr.point[jj].x, linestr.point[jj].y,
									   px0,py0,a0,b0,nx0, true ) ) {
				return true;
			}
		}
	}

	return false;
}


//////////////////////////// 2D line  //////////////////////////////////////////////////
bool JagGeo::lineIntersectTriangle( double x10, double y10, double x20, double y20, 
			                         double x1, double y1, double x2, double y2, double x3, double y3, bool strict )

{
	if ( jagmax(x10, x20) < jagMin(x1, x2, x3) || jagmin(x10, x20) > jagMax(x1,x2,x3) ) {
		prt(("s3228 false\n" ));
		return false;
	}
	if ( jagmax(y10, y20) < jagMin(y1, y2, y3) || jagmin(y10, y20) > jagMax(y1,y2,y3) ) {
		prt(("s3223 false\n" ));
		return false;
	}

	if ( lineIntersectLine(x10,y10,x20,y20, x1, y1, x2, y2 ) ) return true;
	if ( lineIntersectLine(x10,y10,x20,y20, x2, y2, x3, y3 ) ) return true;
	if ( lineIntersectLine(x10,y10,x20,y20, x3, y3, x1, y1 ) ) return true;
	return false;
}

bool JagGeo::lineIntersectLineString( double x10, double y10, double x20, double y20,
							          const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict )
{
	// 2 points are some two neighbor points in sp2
	int start = 0;
	if ( mk2 == JAG_OJAG ) {
		start = 1;
	}

    double dx1, dy1, dx2, dy2;
    const char *str;
    char *p;
	//prt(("s6790 start=%d len=%d  square: x0=%f y0=%f r=%f\n", start, sp1.length(), x0,y0,r ));
	for ( int i=start; i < sp2.length()-1; ++i ) {
		str = sp2[i].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx1, dy1 );
		str = sp2[i+1].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx2, dy2 );
		if ( lineIntersectLine( x10,y10, x20,y20, dx1,dy1,dx2,dy2 ) ) return true;
	}
	return false;
}



bool JagGeo::lineIntersectRectangle( double x10, double y10, double x20, double y20, 
                                         double x0, double y0, double a, double b, double nx, bool strict )
{
	if ( ! validDirection(nx) ) return false;
	if ( pointWithinRectangle( x10, y10, x0, y0,a,b,nx,false) ) return true;
	if ( pointWithinRectangle( x20, y20, x0, y0,a,b,nx,false) ) return true;

	JagLine2D line[4];
	edgesOfRectangle( a, b, line );
	double gx1,gy1, gx2,gy2;
	for ( int i=0; i < 4; ++i ) {
		transform2DCoordLocal2Global( x0, y0, line[i].x1, line[i].y1, nx, gx1,gy1 );
		transform2DCoordLocal2Global( x0, y0, line[i].x2, line[i].y2, nx, gx2,gy2 );
		if ( lineIntersectLine( x10,y10,x20,y20, gx1,gy1,gx2,gy2 ) ) return true;
	}
	return false;
}

bool JagGeo::lineIntersectEllipse( double x10, double y10, double x20, double y20, 
									double x0, double y0, double a, double b, double nx, bool strict )
{
	if ( ! validDirection(nx) ) return false;
	if ( jagIsZero(a) || jagIsZero(b) ) return false;

	double tri_x[2], tri_y[2];
	tri_x[0] = x10; tri_y[0] = y10;
	tri_x[1] = x20; tri_y[1] = y20;
	double loc_x, loc_y;
	for ( int i=0; i < 2; ++i ) {
    	transform2DCoordGlobal2Local( x0, y0, tri_x[i], tri_y[i], nx, loc_x, loc_y );
		if ( pointWithinNormalEllipse( loc_x, loc_y, a, b, strict ) ) { return true; }
	}

	int rel = relationLineEllipse( x10, y10, x20, y20, x0, y0, a, b, nx );
	if ( rel > 0 ) return true;
	return false;
}

bool JagGeo::lineIntersectPolygon( double x10, double y10, double x20, double y20, 
										const AbaxDataString &mk2, const JagStrSplit &sp2 ) 
{
	double X1, Y1, R1x, R1y;
	X1 = ( x10+x20)/2.0; R1x = fabs(x10-x20)/2.0;
	Y1 = ( y10+y20)/2.0; R1y = fabs(y10-y20)/2.0;
	double bbx, bby, rx, ry;
	getPolygonBound( mk2, sp2, bbx, bby, rx, ry );
	if ( bound2DDisjoint(  X1, Y1, R1x, R1y,  bbx, bby, rx, ry ) ) return false;

	JagPolygon pgon;
	int rc = JagParser::addPolygonData( pgon, sp2, false );
	if ( rc < 0 ) return false;

	if ( pointWithinPolygon( x10, y10, pgon ) ) { return true; }
	if ( pointWithinPolygon( x20, y20, pgon ) ) { return true; }

	int jj;
	for ( int i=0; i < pgon.size(); ++i ) {
		const JagLineString3D &linestr = pgon.linestr[i];
		for ( int j=0; j < linestr.size(); ++j ) {
			jj = ( j + 1) % linestr.size();
			if ( lineIntersectLine( linestr.point[j].x, linestr.point[j].y, 
								    linestr.point[jj].x, linestr.point[jj].y,
								    x10,y10, x20,y20 ) ) {
				return true;
			}
		}
	}

	return false;

}


//////////////////////////// 2D triangle //////////////////////////////////////////////////
bool JagGeo::triangleIntersectTriangle( double x10, double y10, double x20, double y20, double x30, double y30,
			                         double x1, double y1, double x2, double y2, double x3, double y3, bool strict )

{
	double X1, Y1, R1x, R1y;
	double X2, Y2, R2x, R2y;
	triangleRegion( x10, y10, x20, y20, x30, y30, X1, Y1, R1x, R1y );
	triangleRegion( x1, y1, x2, y2, x3, y3, X2, Y2, R2x, R2y );
	if ( bound2DDisjoint(  X1, Y1, R1x, R1y, X2, Y2, R2x, R2y ) ) return false;

	JagLine2D line1[3];
	edgesOfTriangle( x10, y10, x20, y20, x30, y30, line1 );
	JagLine2D line2[3];
	edgesOfTriangle( x1, y1, x2, y2, x3, y3, line2 );
	for ( int i=0; i <3; ++i ) {
		for ( int j=0; j <3; ++j ) {
			if ( lineIntersectLine( line1[i], line2[j] ) ) {
				return true;
			}
		}
	}
	return false;
}

bool JagGeo::triangleIntersectLine( double x10, double y10, double x20, double y20, double x30, double y30,
			                         double x1, double y1, double x2, double y2 )

{
	double X1, Y1, R1x, R1y;
	double X2, Y2, R2x, R2y;
	triangleRegion( x10, y10, x20, y20, x30, y30, X1, Y1, R1x, R1y );

	double trix, triy, rx, ry;
	trix = (x1+x2)/2.0;
	triy = (y1+y2)/2.0;
	rx = fabs( trix - jagmin(x1,x2) );
	ry = fabs( triy - jagmin(y1,y2) );
	if ( bound2DDisjoint(  X1, Y1, R1x, R1y, trix, triy, rx, ry ) ) return false;

	JagLine2D line1[3];
	edgesOfTriangle( x10, y10, x20, y20, x30, y30, line1 );
	JagLine2D line2;
	line2.x1 = x1; line2.y1 = y1;
	line2.x2 = x2; line2.y2 = y2;
	for ( int i=0; i <3; ++i ) {
		if ( lineIntersectLine( line1[i], line2 ) ) {
			return true;
		}
	}
	return false;
}

bool JagGeo::triangleIntersectRectangle( double x10, double y10, double x20, double y20, double x30, double y30,
                                         double x0, double y0, double a, double b, double nx, bool strict )
{
	if ( ! validDirection(nx) ) return false;
	return rectangleIntersectTriangle( x0,y0,a,b,nx,  x10,y10, x20,y20, x30, y30, false );
}

bool JagGeo::triangleIntersectEllipse( double x10, double y10, double x20, double y20, double x30, double y30,
									double x0, double y0, double a, double b, double nx, bool strict )

{
	if ( ! validDirection(nx) ) return false;
	if ( jagIsZero(a) || jagIsZero(b) ) return false;

	double X1, Y1, R1x, R1y;
	triangleRegion( x10, y10, x20, y20, x30, y30, X1, Y1, R1x, R1y );
	if ( bound2DDisjoint(  X1, Y1, R1x, R1y, x0,y0,a,b ) ) return false;

	JagLine2D line[3];
	edgesOfTriangle( x10, y10, x20, y20, x30, y30, line);
	double loc_x, loc_y;
	for ( int i=0; i < 3; ++i ) {
    	transform2DCoordGlobal2Local( x0, y0, line[i].x1, line[i].y1, nx, loc_x, loc_y );
		if ( pointWithinNormalEllipse( loc_x, loc_y, a, b, strict ) ) { return true; }
	}
	int rel;
	for ( int i=0; i<3; ++i ) {
		rel = relationLineEllipse( line[i].x1, line[i].y1, line[i].x2, line[i].y2, x0, y0, a, b, nx );
		if ( rel > 0 ) return true;
	}
	return false;
}

bool JagGeo::triangleIntersectPolygon( double x10, double y10, double x20, double y20, double x30, double y30,
										const AbaxDataString &mk2, const JagStrSplit &sp2 ) 
{
	double X1, Y1, R1x, R1y;
	triangleRegion( x10, y10, x20, y20, x30, y30, X1, Y1, R1x, R1y );
	double bbx, bby, rx, ry;
	getPolygonBound( mk2, sp2, bbx, bby, rx, ry );
	if ( bound2DDisjoint(  X1, Y1, R1x, R1y,  bbx, bby, rx, ry ) ) {
		//prt(("s8383 bound2DDisjoint\n" ));
		return false;
	}

	JagPolygon pgon;
	int rc;
	//sp2.print();
	if ( mk2 == JAG_OJAG ) {
		//prt(("s4123 JAG_OJAG sp2: prnt\n" ));
		//sp2.print();
		rc = JagParser::addPolygonData( pgon, sp2, false );
	} else {
		// form linesrting3d  from pdata
		char *p = secondTokenStart( sp2.c_str() );
		//prt(("s4110 p=[%s]\n", p ));
		rc = JagParser::addPolygonData( pgon, p, false, false );
	}

	if ( rc < 0 ) {
		//prt(("s6338 triangleIntersectPolygon addPolygonData rc=%d false\n", rc ));
		return false;
	}

	if ( pointWithinPolygon( x10, y10, pgon ) ) { return true; }
	if ( pointWithinPolygon( x20, y20, pgon ) ) { return true; }
	if ( pointWithinPolygon( x30, y30, pgon ) ) { return true; }

	int jj;
	for ( int i=0; i < pgon.size(); ++i ) {
		const JagLineString3D &linestr = pgon.linestr[i];
		for ( int j=0; j < linestr.size(); ++j ) {
			jj = ( j + 1) % linestr.size();
			if ( lineIntersectTriangle( linestr.point[j].x, linestr.point[j].y, 
									   linestr.point[jj].x, linestr.point[jj].y,
									   x10,y10, x20,y20, x30,y30, false ) ) {
				return true;
			}
		}
	}

	//prt(("s8371 false\n" ));
	return false;

}

bool JagGeo::triangleIntersectLineString( double x10, double y10, double x20, double y20, double x30, double y30,
										const AbaxDataString &mk2, const JagStrSplit &sp2 ) 
{
	double X1, Y1, R1x, R1y;
	triangleRegion( x10, y10, x20, y20, x30, y30, X1, Y1, R1x, R1y );
	#if 0
	prt(("s3420 triangleIntersectLineString x10=%.2f y10=%.2f x20=%.2f y20=%.2f x30=%.2f y30=%.2f\n",
		  x10, y10, x20, y20, x30, y30 ));
	  #endif

	double bbx, bby, rx, ry;
	getLineStringBound( mk2, sp2, bbx, bby, rx, ry );

	if ( bound2DDisjoint(  X1, Y1, R1x, R1y,  bbx, bby, rx, ry ) ) {
		#ifdef DEVDEBUG
		prt(("X1=%.2f Y1=%.2f R1x=%.2 R1y=%.2f\n",  X1, Y1, R1x, R1y ));
		prt((" bbx=%.2f, bby=%.2f, rx=%.2f, ry=%.2f\n",  bbx, bby, rx, ry ));
		prt(("s7918 triangleIntersectLineString bound2DDisjoint return false\n" ));
		#endif
		return false;
	}

	JagLineString linestr;
	JagParser::addLineStringData( linestr, sp2 );

	for ( int j=0; j < linestr.size()-1; ++j ) {
		/**
		prt(("s2287 lp1x=%s lp1y=%s  lp2x=%s lp2y=%s\n", linestr.point[j].x, linestr.point[j].y,
		       linestr.point[j+1].x, linestr.point[j+1].y  ));
	   ***/
		if ( lineIntersectTriangle( jagatof(linestr.point[j].x), jagatof(linestr.point[j].y), 
								   jagatof(linestr.point[j+1].x), jagatof(linestr.point[j+1].y),
								   x10,y10, x20,y20, x30,y30, false ) ) {
			//prt(("s8632 j=%d line intersect triangle\n", j ));
			return true;
		}
	}

	return false;

}

///////////////////////// 2D ellipse
bool JagGeo::ellipseIntersectTriangle( double px0, double py0, double a0, double b0, double nx0,
									double x1, double y1, double x2, double y2, double x3, double y3, bool strict )
{
	return triangleIntersectEllipse( x1, y1, x2, y2, x3, y3, px0, py0, a0, b0, nx0, false );
}

bool JagGeo::ellipseIntersectRectangle( double px0, double py0, double a0, double b0, double nx0,
                                	double x0, double y0, double a, double b, double nx, bool strict )
{
	return rectangleIntersectEllipse( x0, y0, a, b, nx, px0,py0, a0, b0, nx0, false );
}

bool JagGeo::ellipseIntersectEllipse( double px0, double py0, double a0, double b0, double nx0,
                                	double x0, double y0, double a, double b, double nx, bool strict )
{
	if ( ! validDirection(nx0) ) return false;
	if ( ! validDirection(nx) ) return false;
	if ( bound2DDisjoint( px0, py0, a0, b0, x0, y0, a, a ) ) return false;

	if ( pointWithinEllipse( px0, py0, x0, y0, a, b, nx, false ) ) {
		return true;
	}
	if ( pointWithinEllipse( x0, y0, px0, py0, a0, b0, nx0, false ) ) {
		return true;
	}

	JagVector<JagPoint2D> vec;
	double loc_x, loc_y;
	samplesOn2DEllipse( px0, py0, a0, b0, nx0, NUM_SAMPLE, vec );
	for ( int i=0; i <vec.size(); ++i ) {
    	transform2DCoordGlobal2Local( x0, y0, vec[i].x, vec[i].y, nx, loc_x, loc_y );
		if ( pointWithinNormalEllipse( loc_x, loc_y, a, b, strict ) ) { return true; }
	}
	return false;
}

bool JagGeo::ellipseIntersectPolygon( double px0, double py0, double a0, double b0, double nx0,
									  const AbaxDataString &mk2, const JagStrSplit &sp2 )
{
	if ( ! validDirection(nx0) ) return false;
	double bbx, bby, rx, ry;
	getPolygonBound( mk2, sp2, bbx, bby, rx, ry );
	if ( bound2DDisjoint( px0,py0,a0,b0,  bbx, bby, rx, ry ) ) return false;

	JagPolygon pgon;
	int rc = JagParser::addPolygonData( pgon, sp2, false );
	if ( rc < 0 ) return false;

	JagVector<JagPoint2D> vec;
	samplesOn2DEllipse( px0, py0, a0, b0, nx0, NUM_SAMPLE, vec );
	for ( int i=0; i <vec.size(); ++i ) {
		if ( pointWithinPolygon( vec[i].x, vec[i].y, pgon  ) ) { return true; }
	}
	return false;
}

///////////////////////////////////// rectangle 3D /////////////////////////////////
bool JagGeo::rectangle3DIntersectBox(  double px0, double py0, double pz0, double a0, double b0,
								double nx0, double ny0,
						        double x, double y, double z, 
							    double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if ( bound3DDisjoint( px0, py0, pz0, a0,b0, jagmax(a0,b0), x, y, z, w, d, h ) ) return false;

	JagLine3D line1[4];
	edgesOfRectangle3D( a0, b0, line1 );
	transform3DLinesLocal2Global( px0, py0, pz0, nx0, ny0, 4, line1 );

	JagLine3D line2[12];
	edgesOfBox( w, d, h, line2 );
	transform3DLinesLocal2Global( x, y, z, nx, ny, 12, line2 );

	for ( int i=0;i<4;++i) {
		for ( int j=0;j<12;++j) {
			if ( line3DIntersectLine3D( line1[i], line2[j] )  ) {
				return true;
			}
		}
	}
	return false;
}

bool JagGeo::rectangle3DIntersectEllipsoid(  double px0, double py0, double pz0, double a0, double b0,
									double nx0, double ny0,
						        	double x, double y, double z, 
							    	double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if ( bound3DDisjoint( px0, py0, pz0, a0,b0, jagmax(a0,b0), x, y, z, w, d, h ) ) return false;

	if ( point3DWithinEllipsoid( px0, py0, pz0, x, y, z, w, d, h, nx, ny, false ) ) {
		return true;
	}

	// rect 4 corners in ellipsoid
	JagPoint3D corn[4];
	cornersOfRectangle3D( a0, b0, corn);
	transform3DEdgesLocal2Global( px0, py0, pz0, nx0, ny0, 4, corn );
	for ( int i=0; i<4; ++i ) {
		if ( point3DWithinEllipsoid(corn[i].x, corn[i].y, corn[i].z, x, y, z, w, d, h, nx, ny, false ) ) {
			return true;
		}
	}

	// on ellipsoid
	bool rc = triangle3DIntersectEllipsoid(  corn[0].x, corn[0].y, corn[0].z,
									corn[1].x,  corn[1].y,  corn[1].z,
									 corn[2].x, corn[2].y, corn[2].z,
									 x,y,z, w,d,h, nx, ny, false );
	if ( rc ) return true;
	return false;
}

bool JagGeo::rectangle3DIntersectCone(  double px0, double py0, double pz0, double a0, double b0,
									double nx0, double ny0,
						        	double x, double y, double z, 
							    	double r, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if ( bound3DDisjoint( px0, py0, pz0, a0,b0,b0, x, y, z, r, r, h ) ) return false;

	JagPoint3D point[4];
	point[0].x = -a0; point[0].y = -b0; point[0].z = 0.0;
	point[1].x = -a0; point[1].y = b0; point[1].z = 0.0;
	point[2].x = a0; point[2].y = b0; point[2].z = 0.0;
	point[3].x = a0; point[3].y = -b0; point[3].z = 0.0;
	double sq_x, sq_y, sq_z;
	for ( int i=0; i < 4; ++i ) {
		// sq_x, sq_y, sq_z are coords in x-y-z of each cube corner node
		transform3DCoordLocal2Global( px0, py0, pz0, point[i].x, point[i].y, point[i].z, nx0, ny0, sq_x, sq_y, sq_z );
		// transform3DCoordGlobal2Local( x, y, z, sq_x, sq_y, sq_z, nx, ny, loc_x, loc_y, loc_z );
		// loc_x, loc_y, loc_z are within second cube sys
		if (  point3DWithinCone(  sq_x, sq_y, sq_z, x,y,z, r,h, nx, ny, strict ) ) {
			return true;
		}
	}

	bool rc =  triangle3DIntersectCone(  
							point[0].x, point[0].y, point[0].z,
							point[1].x, point[1].y, point[1].z,
							point[2].x, point[2].y, point[2].z,
							x,y,z, r,h, nx,ny, false );
	return rc;
}


///////////////////////////////////// line 3D /////////////////////////////////
bool JagGeo::line3DIntersectLineString3D( double x10, double y10, double z10, double x20, double y20, double z20,
							          const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict )
{
	// 2 points are some two neighbor points in sp2
	int start = 0;
	if ( mk2 == JAG_OJAG ) {
		start = 1;
	}

    double dx1, dy1, dz1, dx2, dy2, dz2;
    const char *str;
    char *p;
	//prt(("s6790 start=%d len=%d  square: x0=%f y0=%f r=%f\n", start, sp1.length(), x0,y0,r ));
	for ( int i=start; i < sp2.length()-1; ++i ) {
		str = sp2[i].c_str();
		if ( strchrnum( str, ':') < 2 ) continue;
		get3double(str, p, ':', dx1, dy1, dz1 );
		str = sp2[i+1].c_str();
		if ( strchrnum( str, ':') < 2 ) continue;
		get3double(str, p, ':', dx2, dy2, dz2 );
		if ( line3DIntersectLine3D( x10,y10,z10, x20,y20,z20, dx1,dy1,dz1,dx2,dy2,dz2 ) ) return true;
	}
	return false;
}
bool JagGeo::line3DIntersectBox( const JagLine3D &line, double x0, double y0, double z0,
				double w, double d, double h, double nx, double ny, bool strict )
{
	return line3DIntersectBox( line.x1, line.y1, line.z1, line.x2, line.y2, line.z2,
										x0,y0,z0,w,d,h, nx, ny, false );
}


bool JagGeo::line3DIntersectBox(  double x10, double y10, double z10, 
								   double x20, double y20, double z20, 
									double x0, double y0, double z0,
									double w, double d, double h, 
									double nx, double ny, bool strict )
{
	if ( bound3DLineBoxDisjoint(  x10, y10, z10, x20, y20, z20, x0, y0, z0, w, d, h ) ) return false;
	if ( point3DWithinBox( x10, y10, z10, x0, y0, z0, w, d, h, nx, ny, false ) ) return true;
	if ( point3DWithinBox( x20, y20, z20, x0, y0, z0, w, d, h, nx, ny, false ) ) return true;

	JagLine3D line0;
	line0.x1 = x10; line0.y1 = y10; line0.z1 = z10;
	line0.x2 = x20; line0.y2 = y20; line0.z2 = z20;

	JagRectangle3D rect[6];
	surfacesOfBox( w,d,h, rect); // local rectangles
	for ( int i=0; i < 6; ++i ) {
		rect[i].transform( x0, y0, z0, nx, ny ); // global 
		if ( line3DIntersectRectangle3D( line0, rect[i] ) ) return true;
	}
	return false;
}

bool JagGeo::line3DIntersectSphere(  double x10, double y10, double z10, double x20, double y20, double z20, 
 									  double x, double y, double z, double r, bool strict )
{
	if ( bound3DLineBoxDisjoint( x10, y10, z10, x20, y20, z20, x, y, z, r, r, r ) ) return false;
	if ( point3DWithinSphere( x10,y10,z10, x, y, z, r, false ) ) return true;
	if ( point3DWithinSphere( x20,y20,z20, x, y, z, r, false ) ) return true;

	int rel = relationLine3DSphere( x10,y10,z10,x20,y20,z20, x,y,z, r);
	if ( rel > 0 ) return true;
	return false;
}


bool JagGeo::line3DIntersectEllipsoid(  double x10, double y10, double z10, double x20, double y20, double z20,
								 double x0, double y0, double z0,
								double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx, ny) ) return false;
	if ( bound3DLineBoxDisjoint(  x10, y10, z10, x20, y20, z20, x0, y0, z0, w, d, h ) ) return false;
	if ( point3DWithinEllipsoid( x10,y10,z10, x0, y0, z0, w,d,h, nx,ny, false ) ) return true;
	if ( point3DWithinEllipsoid( x20,y20,z20, x0, y0, z0, w,d,h, nx,ny, false ) ) return true;
	int rel = relationLine3DEllipsoid( x10, y10, z10, x20, y20, z20,
                                       x0, y0, z0, w, d, h, nx, ny );
	if ( rel > 0 ) return true;
	return false;
}

bool JagGeo::line3DIntersectCone(  double x10, double y10, double z10, double x20, double y20, double z20,
								 double x0, double y0, double z0,
								double r, double h, double nx, double ny, bool strict )
{
	if ( bound3DLineBoxDisjoint(  x10, y10, z10, x20, y20, z20, x0, y0, z0, r, r, h ) ) return false;
	if ( point3DWithinCone( x10,y10,z10, x0, y0, z0, r,h, nx,ny, false ) ) return true;
	if ( point3DWithinCone( x20,y20,z20, x0, y0, z0, r,h, nx,ny, false ) ) return true;
	int rel = relationLine3DCone( x10, y10, z10, x20, y20, z20, x0, y0, z0, r, h, nx, ny );
	if ( rel > 0 ) return true;
	return false;
}

bool JagGeo::line3DIntersectCylinder(  double x10, double y10, double z10, double x20, double y20, double z20,
								 double x0, double y0, double z0,
								double a, double b, double c, double nx, double ny, bool strict )
{
	if ( bound3DLineBoxDisjoint(  x10, y10, z10, x20, y20, z20, x0, y0, z0, a, b, c ) ) return false;
	if ( point3DWithinCylinder( x10,y10,z10, x0, y0, z0, a,c, nx,ny, false ) ) return true;
	if ( point3DWithinCylinder( x20,y20,z20, x0, y0, z0, a,c, nx,ny, false ) ) return true;
	int rel = relationLine3DCylinder( x10, y10, z10, x20, y20, z20, x0, y0, z0, a,b,c, nx, ny );
	if ( rel > 0 ) return true;
	return false;
}


// lineString3D intersect
bool JagGeo::lineString3DIntersectLineString3D( const AbaxDataString &mk1, const JagStrSplit &sp1,
			                                const AbaxDataString &mk2, const JagStrSplit &sp2 )
{
	// sweepline algo
	int start1 = 0;
	if ( mk1 == JAG_OJAG ) { start1 = 1; }
	int start2 = 0;
	if ( mk2 == JAG_OJAG ) { start2 = 1; }


	double dx1, dy1, dz1, dx2, dy2, dz2, t;
	const char *str;
	char *p; int i;
	int totlen = sp1.length()-start1 + sp2.length() - start2;
	JagSortPoint3D *points = new JagSortPoint3D[2*totlen];
	int j = 0;
	int rc;
	for ( i=start1; i < sp1.length()-1; ++i ) {
		str = sp1[i].c_str();
		if ( strchrnum( str, ':') < 2 ) continue;
		get3double(str, p, ':', dx1, dy1, dz1 );

		str = sp1[i+1].c_str();
		if ( strchrnum( str, ':') < 2 ) continue;
		get3double(str, p, ':', dx2, dy2, dz2 );

		if ( jagEQ(dx1, dx2)) {
			dx2 += 0.000001f;
		}

		if ( dx1 > dx2 ) {
			// swap
			t = dx1; dx1 = dx2; dx2 = t; 
			t = dy1; dy1 = dy2; dy2 = t; 
		}

		points[j].x1 = dx1; points[j].y1 = dy1; points[j].z1 = dz1;
		points[j].x2 = dx2; points[j].y2 = dy2; points[j].z2 = dz2;
		points[j].end = JAG_LEFT;
		points[j].color = JAG_RED;
		++j;

		points[j].x1 = dx1; points[j].y1 = dy1; points[j].z1 = dz1;
		points[j].x2 = dx2; points[j].y2 = dy2; points[j].z2 = dz2;
		points[j].end = JAG_RIGHT;
		points[j].color = JAG_RED;
		++j;
	}

	for ( i=start2; i < sp2.length()-1; ++i ) {
		str = sp2[i].c_str();

		if ( strchrnum( str, ':') < 2 ) continue;
		get3double(str, p, ':', dx1, dy1, dz1 );

		str = sp2[i+1].c_str();
		if ( strchrnum( str, ':') < 2 ) continue;
		get3double(str, p, ':', dx2, dy2, dz2 );

		if ( jagEQ(dx1, dx2) )  {
			dx2 += 0.000001f;
		}

		if ( dx1 > dx2 ) {
			// swap
			t = dx1; dx1 = dx2; dx2 = t; 
			t = dy1; dy1 = dy2; dy2 = t; 
		}

		points[j].x1 = dx1; points[j].y1 = dy1; points[j].z1 = dz1;
		points[j].x2 = dx2; points[j].y2 = dy2; points[j].z2 = dz2;
		points[j].end = JAG_LEFT;
		points[j].color = JAG_BLUE;
		++j;

		points[j].x1 = dx1; points[j].y1 = dy1; points[j].z1 = dz1;
		points[j].x2 = dx2; points[j].y2 = dy2; points[j].z2 = dz2;
		points[j].end = JAG_RIGHT;
		points[j].color = JAG_BLUE;
		++j;

	}

	int len = j;
	rc = sortLinePoints( points, len );
	if ( rc ) {
		//prt(("s7732 sortIntersectLinePoints rc=%d retur true intersect\n", rc ));
		// return true;
	}

	JagArray<JagLineSeg3DPair> *jarr = new JagArray<JagLineSeg3DPair>();
	const JagLineSeg3DPair *above;
	const JagLineSeg3DPair *below;
	JagLineSeg3DPair seg; seg.value = '1';
	for ( int i=0; i < len; ++i ) {
		seg.key.x1 =  points[i].x1; seg.key.y1 =  points[i].y1; seg.key.z1 =  points[i].z1;
		seg.key.x2 =  points[i].x2; seg.key.y2 =  points[i].y2; seg.key.z2 =  points[i].z2;
		seg.color = points[i].color;
		//prt(("s0088 seg print:\n" ));
		//seg.print();

		if ( JAG_LEFT == points[i].end ) {
			jarr->insert( seg );
			above = jarr->getSucc( seg );
			below = jarr->getPred( seg );
			/***
			if ( above ) {
				prt(("s7781 above print: \n" ));
				above->print();
			}
			if ( below ) {
				prt(("s7681 below print: \n" ));
				below->print();
			}
			***/

			if ( above && *above == JagLineSeg3DPair::NULLVALUE ) {
				prt(("s6251 above is NULLVALUE abort\n" ));
				//abort();
				above = NULL;
			}

			if ( below && *below == JagLineSeg3DPair::NULLVALUE ) {
				prt(("s2598 below is NULLVALUE abort\n" ));
				//abort();
				below = NULL;
			}

			if ( above && below ) {
				if ( above->color != below->color 
				     && line3DIntersectLine3D( above->key.x1,above->key.y1,above->key.z1, 
											   above->key.x2,above->key.y2, above->key.z2,
									           below->key.x1,below->key.y1,below->key.z1,
											   below->key.x2,below->key.y2, below->key.z2 ) ) {
					prt(("s7440 left above below intersect\n" ));
					return true;
				}
			} else if ( above ) {
				if ( above->color != seg.color 
				     && line3DIntersectLine3D( above->key.x1,above->key.y1,above->key.z1,
					 						   above->key.x2,above->key.y2,above->key.z2,
									    	   seg.key.x1,seg.key.y1,seg.key.z1,
											   seg.key.x2,seg.key.y2,seg.key.z2 ) ) {
					prt(("s7341 left above seg intersect\n" ));
					return true;
				}
			} else if ( below ) {
				if ( below->color != seg.color 
				    && line3DIntersectLine3D( below->key.x1,below->key.y1,below->key.z1,
											  below->key.x2,below->key.y2,below->key.z2,
									    	  seg.key.x1,seg.key.y1,seg.key.z1,
											  seg.key.x2,seg.key.y2,seg.key.z2 ) ) {
					prt(("s7611 left below seg intersect\n" ));
					return true;
				}
			}
		} else {
			// right end
			above = jarr->getSucc( seg );
			below = jarr->getPred( seg );
			if ( above && *above == JagLineSeg3DPair::NULLVALUE ) {
				prt(("s7211 above is NULLVALUE abort\n" ));
				//abort();
				above = NULL;
			}

			if ( below && *below == JagLineSeg3DPair::NULLVALUE ) {
				#if 0
				prt(("7258 below is NULLVALUE abort\n" ));
				jarr->printKey();
				seg.println();
				//abort();
				#endif
				below = NULL;
			}

			if ( above && below ) {
				if ( above->color != below->color 
					 && line3DIntersectLine3D( above->key.x1,above->key.y1,above->key.z1,
					 						   above->key.x2,above->key.y2,above->key.z2,
									    	   below->key.x1,below->key.y1,below->key.z1,
											   below->key.x2,below->key.y2,below->key.z2 ) ) {
					prt(("s7043 rightend above below intersect\n" ));
					return true;
				}
			} else if ( above ) {
				if ( above->color != seg.color 
					 && line3DIntersectLine3D( above->key.x1,above->key.y1,above->key.z1,
					 						   above->key.x2,above->key.y2,above->key.z2,
									    	   seg.key.x1,seg.key.y1,seg.key.z1,
											   seg.key.x2,seg.key.y2,seg.key.z2 ) ) {
					prt(("s7710 rightend above seg intersect\n" ));
					return true;
				}
			} else if ( below ) {
				if ( below->color != seg.color 
					 && line3DIntersectLine3D( below->key.x1,below->key.y1,below->key.z1,
					 						   below->key.x2,below->key.y2,below->key.z2,
									    	   seg.key.x1,seg.key.y1,seg.key.z1,
											   seg.key.x2,seg.key.y2,seg.key.z2) ) {
					prt(("s4740 rightend below seg intersect\n" ));
					return true;
				}
			}
			jarr->remove( seg );
		}
	}

	delete [] points;
	delete jarr;

	prt(("s7810 no intersect\n"));
	return false;
}

bool JagGeo::lineString3DIntersectBox(  const AbaxDataString &mk1, const JagStrSplit &sp1,
									double x0, double y0, double z0,
									double w, double d, double h, 
									double nx, double ny, bool strict )
{
	// prt(("s6752 lineString3DIntersectBox ..\n" ));
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
		/***
		prt(("s6753 sp1: bbx=%f bby=%f bbz=%f    brx=%f bry=%f brz=%f\n",
				bbx, bby, bbz, brx, bry, brz ));
		prt(("s6753 x0=%f y0=%f z0=%f w=%f d=%f h=%f\n", x0, y0, z0,  w, d, h ));
		***/

        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  w, d, h ) ) {
			// prt(("s6750 bound3DDisjoint return false\n" ));
            return false;
        }
        start = 1;
    }

	JagRectangle3D rect[6];
	surfacesOfBox( w,d,h, rect); // local rectangles
	for ( int i=0; i < 6; ++i ) {
		rect[i].transform( x0, y0, z0, nx, ny ); // global 
	}

    double dx1, dy1, dz1, dx2, dy2, dz2;
	JagLine3D line3d;
    const char *str;
    char *p; int i;
    for ( i=start; i < sp1.length()-1; ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx1, dy1, dz1 );
		if ( point3DWithinBox( dx1, dy1, dz1,  x0, y0, z0, w,d,h,nx,ny, strict ) ) return true;
        str = sp1[i+1].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx2, dy2, dz2 );
		line3d.x1=dx1; line3d.y1=dy1; line3d.z1=dz1;
		line3d.x2=dx2; line3d.y2=dy2; line3d.z2=dz2;
		for ( int j=0; j<6; ++j ) {
			if ( line3DIntersectRectangle3D( line3d, rect[i] ) ) return true;
		}
    }

    str = sp1[i].c_str();
    if ( strchrnum( str, ':') >= 2 ) {
        get3double(str, p, ':', dx1, dy1, dz1 );
		if ( point3DWithinBox( dx1, dy1, dz1,  x0, y0, z0, w,d,h, nx,ny, strict ) ) return true;
	}

	prt(("s6750 bound3DDisjoint return false\n" ));
	return false;
}

bool JagGeo::lineString3DIntersectSphere(  const AbaxDataString &mk1, const JagStrSplit &sp1,
 									  double x0, double y0, double z0, double r, bool strict )
{
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  r, r, r ) ) {
            return false;
        }
        start = 1;
    }

    double dx1, dy1, dz1, dx2, dy2, dz2;
    const char *str;
    char *p; int i;
    for ( i=start; i < sp1.length()-1; ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx1, dy1, dz1 );
		if ( point3DWithinSphere( dx1, dy1, dz1,  x0, y0, z0, r, strict ) ) return true;
        str = sp1[i+1].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx2, dy2, dz2 );
		if ( line3DIntersectSphere(  dx1,dy1,dz1,dx2,dy2,dz2, x0, y0, z0, r, strict ) ) return true;
    }

    str = sp1[i].c_str();
    if ( strchrnum( str, ':') >= 2 ) {
        get3double(str, p, ':', dx1, dy1, dz1 );
		if ( point3DWithinSphere( dx1, dy1, dz1,  x0, y0, z0, r, strict ) ) return true;
	}

	return false;
}


bool JagGeo::lineString3DIntersectEllipsoid(  const AbaxDataString &mk1, const JagStrSplit &sp1,
								 double x0, double y0, double z0,
								double w, double d, double h, double nx, double ny, bool strict )
{
	/****
	if ( ! validDirection(nx, ny) ) return false;
	if ( bound3DLineBoxDisjoint(  x10, y10, z10, x20, y20, z20, x0, y0, z0, w, d, h ) ) return false;
	if ( point3DWithinEllipsoid( x10,y10,z10, x0, y0, z0, w,d,h, nx,ny, false ) ) return true;
	if ( point3DWithinEllipsoid( x20,y20,z20, x0, y0, z0, w,d,h, nx,ny, false ) ) return true;
	int rel = relationLine3DEllipsoid( x10, y10, z10, x20, y20, z20,
                                       x0, y0, z0, w, d, h, nx, ny );
	if ( rel > 0 ) return true;
	return false;
	*****/
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  w, d, h ) ) {
            return false;
        }
        start = 1;
    }

    double dx1, dy1, dz1, dx2, dy2, dz2;
    const char *str;
    char *p; int i;
    for ( i=start; i < sp1.length()-1; ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx1, dy1, dz1 );
		if ( point3DWithinEllipsoid( dx1, dy1, dz1,  x0, y0, z0, w,d,h,nx,ny, strict ) ) return true;
        str = sp1[i+1].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx2, dy2, dz2 );
		if ( line3DIntersectEllipsoid(  dx1,dy1,dz1,dx2,dy2,dz2, x0, y0, z0, w,d,h,nx,ny, strict ) ) return true;
    }

    str = sp1[i].c_str();
    if ( strchrnum( str, ':') >= 2 ) {
        get3double(str, p, ':', dx1, dy1, dz1 );
		if ( point3DWithinEllipsoid( dx1, dy1, dz1,  x0, y0, z0, w,d,h,nx,ny, strict ) ) return true;
	}

	return false;
}

bool JagGeo::lineString3DIntersectCone(  const AbaxDataString &mk1, const JagStrSplit &sp1,
								 double x0, double y0, double z0,
								double r, double h, double nx, double ny, bool strict )
{
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  r, r, h ) ) {
            return false;
        }
        start = 1;
    }

    double dx1, dy1, dz1, dx2, dy2, dz2;
    const char *str;
    char *p; int i;
    for ( int i=start; i < sp1.length()-1; ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx1, dy1, dz1 );
		if ( point3DWithinCone( dx1, dy1, dz1,  x0, y0, z0, r,h,nx,ny, strict ) ) return true;
        str = sp1[i+1].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx2, dy2, dz2 );
		if ( line3DIntersectCone(  dx1,dy1,dz1,dx2,dy2,dz2, x0, y0, z0, r,h, nx,ny, strict ) ) return true;
    }

    str = sp1[i].c_str();
    if ( strchrnum( str, ':') >= 2 ) {
        get3double(str, p, ':', dx1, dy1, dz1 );
		if ( point3DWithinCone( dx1, dy1, dz1,  x0, y0, z0, r,h,nx,ny, strict ) ) return true;
	}
	return false;
}

bool JagGeo::lineString3DIntersectCylinder(  const AbaxDataString &mk1, const JagStrSplit &sp1,
								 double x0, double y0, double z0,
								double a, double b, double c, double nx, double ny, bool strict )
{
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  a, b, c ) ) {
            return false;
        }
        start = 1;
    }

    double dx1, dy1, dz1, dx2, dy2, dz2;
    const char *str;
    char *p; int i;
    for ( i=start; i < sp1.length()-1; ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx1, dy1, dz1 );
		if ( point3DWithinCylinder( dx1, dy1, dz1,  x0, y0, z0, a,c,nx,ny, strict ) ) return true;
        str = sp1[i+1].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx2, dy2, dz2 );
		if ( line3DIntersectCylinder(  dx1,dy1,dz1,dx2,dy2,dz2, x0, y0, z0, a,b,c, nx,ny, strict ) ) return true;
    }

    str = sp1[i].c_str();
    if ( strchrnum( str, ':') >= 2 ) {
        get3double(str, p, ':', dx1, dy1, dz1 );
		if ( point3DWithinCylinder( dx1, dy1, dz1,  x0, y0, z0, a,c,nx,ny, strict ) ) return true;
	}
	return false;
}

bool JagGeo::lineString3DIntersectTriangle3D(  const AbaxDataString &mk1, const JagStrSplit &sp1,
											   const AbaxDataString &mk2, const JagStrSplit &sp2 )
{
	if ( sp2.length() < 9 ) return false;
	double x1 = jagatof( sp2[0].c_str() ); 
	double y1 = jagatof( sp2[1].c_str() ); 
	double z1 = jagatof( sp2[2].c_str() ); 
	double x2 = jagatof( sp2[3].c_str() ); 
	double y2 = jagatof( sp2[4].c_str() ); 
	double z2 = jagatof( sp2[5].c_str() ); 
	double x3 = jagatof( sp2[6].c_str() ); 
	double y3 = jagatof( sp2[7].c_str() ); 
	double z3 = jagatof( sp2[8].c_str() ); 


    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
		double x0, y0, z0, Rx, Ry, Rz;
		triangle3DRegion( x1, y1, z1, x2, y2, z2, x3, y3, z3, x0, y0, z0, Rx, Ry, Rz );
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  Rx, Ry, Rz ) ) {
            return false;
        }
        start = 1;
    }

    double dx1, dy1, dz1, dx2, dy2, dz2;
    const char *str;
    char *p; int i;
	JagLine3D line3d;
	JagPoint3D atPoint;
	JagPoint3D p1(x1,y1,z1);
	JagPoint3D p2(x2,y2,z2);
	JagPoint3D p3(x3,y3,z3);
    for ( i=start; i < sp1.length()-1; ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx1, dy1, dz1 );

		//if ( point3DWithinCylinder( dx1, dy1, dz1,  x0, y0, z0, a,c,nx,ny, strict ) ) return true;
        str = sp1[i+1].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx2, dy2, dz2 );
		//if ( line3DIntersectCylinder(  dx1,dy1,dz1,dx2,dy2,dz2, x0, y0, z0, a,b,c, nx,ny, strict ) ) return true;
		line3d.x1 = dx1; line3d.y1 = dy1; line3d.z1 = dz1;
		line3d.x2 = dx2; line3d.y2 = dy2; line3d.z2 = dz2;
		if ( line3DIntersectTriangle3D( line3d, p1, p2, p3, atPoint ) ) { return true; }
    }

	return false;
}

bool JagGeo::lineString3DIntersectSquare3D(  const AbaxDataString &mk1, const JagStrSplit &sp1,
											   const AbaxDataString &mk2, const JagStrSplit &sp2 )
{
	if ( sp2.length() < 4 ) return false;
	double px0 = jagatof( sp2[0].c_str() ); 
	double py0 = jagatof( sp2[1].c_str() ); 
	double pz0 = jagatof( sp2[2].c_str() ); 
	double pr0 = jagatof( sp2[3].c_str() ); 
	double nx0 = safeget( sp2, 4);
	double ny0 = safeget( sp2, 5);

    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  px0, py0, pz0,  pr0, pr0, pr0 ) ) {
            return false;
        }
        start = 1;
    }

    double dx1, dy1, dz1, dx2, dy2, dz2;
    const char *str;
    char *p; int i;
	JagLine3D line3d;
    for ( i=start; i < sp1.length()-1; ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx1, dy1, dz1 );

        str = sp1[i+1].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx2, dy2, dz2 );

		line3d.x1 = dx1; line3d.y1 = dy1; line3d.z1 = dz1;
		line3d.x2 = dx2; line3d.y2 = dy2; line3d.z2 = dz2;
		if ( line3DIntersectRectangle3D( line3d, px0, py0, pz0, pr0, pr0, nx0, ny0 ) ) {
			return true;
		}
    }

	return false;
}

bool JagGeo::lineString3DIntersectRectangle3D(  const AbaxDataString &mk1, const JagStrSplit &sp1,
											   const AbaxDataString &mk2, const JagStrSplit &sp2 )
{
	if ( sp2.length() < 4 ) return false;
	double px0 = jagatof( sp2[0].c_str() ); 
	double py0 = jagatof( sp2[1].c_str() ); 
	double pz0 = jagatof( sp2[2].c_str() ); 
	double a0 = jagatof( sp2[3].c_str() ); 
	double b0 = jagatof( sp2[3].c_str() ); 
	double nx0 = safeget( sp2, 4);
	double ny0 = safeget( sp2, 5);

    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  px0, py0, pz0,  a0, a0, b0 ) ) {
            return false;
        }
        start = 1;
    }

    double dx1, dy1, dz1, dx2, dy2, dz2;
    const char *str;
    char *p; int i;
	JagLine3D line3d;
    for ( i=start; i < sp1.length()-1; ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx1, dy1, dz1 );

        str = sp1[i+1].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx2, dy2, dz2 );

		line3d.x1 = dx1; line3d.y1 = dy1; line3d.z1 = dz1;
		line3d.x2 = dx2; line3d.y2 = dy2; line3d.z2 = dz2;
		if ( line3DIntersectRectangle3D( line3d, px0, py0, pz0, a0, b0, nx0, ny0 ) ) {
			return true;
		}
    }

	return false;
}


// Polygon3D intersect
bool JagGeo::polygon3DIntersectLineString3D( const AbaxDataString &mk1, const JagStrSplit &sp1,
			                                const AbaxDataString &mk2, const JagStrSplit &sp2 )
{
	// sweepline algo
	int start1 = 0;
	if ( mk1 == JAG_OJAG ) { start1 = 1; }
	int start2 = 0;
	if ( mk2 == JAG_OJAG ) { start2 = 1; }

	//prt(("s8122 polygon3DIntersectLineString3D sp1: sp2:\n" ));
	//sp1.print();
	//sp2.print();

	double dx1, dy1, dz1, dx2, dy2, dz2, t;
	const char *str;
	char *p; int i;
	int rc;
	int totlen = sp1.length()-start1 + sp2.length() - start2;
	JagSortPoint3D *points = new JagSortPoint3D[2*totlen];

	int j = 0;
	for ( i=start1; i < sp1.length()-1; ++i ) {
		str = sp1[i].c_str();
		if ( strchrnum( str, ':') < 2 ) continue;
		get3double(str, p, ':', dx1, dy1, dz1 );

		str = sp1[i+1].c_str();
		if ( strchrnum( str, ':') < 2 ) continue;
		get3double(str, p, ':', dx2, dy2, dz2 );

		if ( jagEQ(dx1, dx2)) {
			dx2 += 0.000001f;
		}

		if ( dx1 > dx2 ) {
			// swap
			t = dx1; dx1 = dx2; dx2 = t; 
			t = dy1; dy1 = dy2; dy2 = t; 
		}

		points[j].x1 = dx1; points[j].y1 = dy1; points[j].z1 = dz1;
		points[j].x2 = dx2; points[j].y2 = dy2; points[j].z2 = dz2;
		points[j].end = JAG_LEFT;
		points[j].color = JAG_RED;
		++j;

		points[j].x1 = dx1; points[j].y1 = dy1; points[j].z1 = dz1;
		points[j].x2 = dx2; points[j].y2 = dy2; points[j].z2 = dz2;
		points[j].end = JAG_RIGHT;
		points[j].color = JAG_RED;
		++j;
	}

	/******
	JagPolygon pgon;
	double t;
	rc = addPolygonData( pgon, sp1, false ); // all polygons
	if ( rc < 0 ) return false;
	for ( int i=0; i < pgon.size(); ++i ) {
		const JagLineString3D &linestr = pgon.linestr[i];
		for ( int j=0; j < linestr.size(); ++j ) {
			dx1 = linestr[j].x1; dy1 = linestr[j].y1;
			dx2 = linestr[j].x2; dy2 = linestr[j].y2;
			if ( jagEQ(dx1, dx2)) { dx2 += 0.000001f; }
			if ( dx1 > dx2 ) {
				t = dx1; dx1 = dx2; dx2 = t; 
				t = dy1; dy1 = dy2; dy2 = t; 
			}
		}
	}
	*******/


	for ( i=start2; i < sp2.length()-1; ++i ) {
		str = sp2[i].c_str();

		if ( strchrnum( str, ':') < 2 ) continue;
		get3double(str, p, ':', dx1, dy1, dz1 );

		str = sp2[i+1].c_str();
		if ( strchrnum( str, ':') < 2 ) continue;
		get3double(str, p, ':', dx2, dy2, dz2 );

		if ( jagEQ(dx1, dx2) )  {
			dx2 += 0.000001f;
		}

		if ( dx1 > dx2 ) {
			// swap
			t = dx1; dx1 = dx2; dx2 = t; 
			t = dy1; dy1 = dy2; dy2 = t; 
		}

		points[j].x1 = dx1; points[j].y1 = dy1; points[j].z1 = dz1;
		points[j].x2 = dx2; points[j].y2 = dy2; points[j].z2 = dz2;
		points[j].end = JAG_LEFT;
		points[j].color = JAG_BLUE;
		++j;

		points[j].x1 = dx1; points[j].y1 = dy1; points[j].z1 = dz1;
		points[j].x2 = dx2; points[j].y2 = dy2; points[j].z2 = dz2;
		points[j].end = JAG_RIGHT;
		points[j].color = JAG_BLUE;
		++j;

	}

	int len = j;
	rc = sortLinePoints( points, len );
	if ( rc ) {
		//prt(("s7732 sortIntersectLinePoints rc=%d retur true intersect\n", rc ));
		// return true;
	}

	JagArray<JagLineSeg3DPair> *jarr = new JagArray<JagLineSeg3DPair>();
	const JagLineSeg3DPair *above;
	const JagLineSeg3DPair *below;
	JagLineSeg3DPair seg; seg.value = '1';
	for ( int i=0; i < len; ++i ) {
		seg.key.x1 =  points[i].x1; seg.key.y1 =  points[i].y1; seg.key.z1 =  points[i].z1;
		seg.key.x2 =  points[i].x2; seg.key.y2 =  points[i].y2; seg.key.z2 =  points[i].z2;
		seg.color = points[i].color;
		//prt(("s0088 seg print:\n" ));
		//seg.print();

		if ( JAG_LEFT == points[i].end ) {
			jarr->insert( seg );
			above = jarr->getSucc( seg );
			below = jarr->getPred( seg );
			/***
			if ( above ) {
				prt(("s7781 above print: \n" ));
				above->print();
			}
			if ( below ) {
				prt(("s7681 below print: \n" ));
				below->print();
			}
			***/

			if ( above && *above == JagLineSeg3DPair::NULLVALUE ) {
				prt(("s6251 above is NULLVALUE abort\n" ));
				//abort();
				above = NULL;
			}

			if ( below && *below == JagLineSeg3DPair::NULLVALUE ) {
				prt(("s2598 below is NULLVALUE abort\n" ));
				//abort();
				below = NULL;
			}

			if ( above && below ) {
				if ( above->color != below->color 
				     && line3DIntersectLine3D( above->key.x1,above->key.y1,above->key.z1, 
											   above->key.x2,above->key.y2, above->key.z2,
									           below->key.x1,below->key.y1,below->key.z1,
											   below->key.x2,below->key.y2, below->key.z2 ) ) {
					prt(("s7440 left above below intersect\n" ));
					return true;
				}
			} else if ( above ) {
				if ( above->color != seg.color 
				     && line3DIntersectLine3D( above->key.x1,above->key.y1,above->key.z1,
					 						   above->key.x2,above->key.y2,above->key.z2,
									    	   seg.key.x1,seg.key.y1,seg.key.z1,
											   seg.key.x2,seg.key.y2,seg.key.z2 ) ) {
					prt(("s7341 left above seg intersect\n" ));
					return true;
				}
			} else if ( below ) {
				if ( below->color != seg.color 
				    && line3DIntersectLine3D( below->key.x1,below->key.y1,below->key.z1,
											  below->key.x2,below->key.y2,below->key.z2,
									    	  seg.key.x1,seg.key.y1,seg.key.z1,
											  seg.key.x2,seg.key.y2,seg.key.z2 ) ) {
					prt(("s7611 left below seg intersect\n" ));
					return true;
				}
			}
		} else {
			// right end
			above = jarr->getSucc( seg );
			below = jarr->getPred( seg );
			if ( above && *above == JagLineSeg3DPair::NULLVALUE ) {
				prt(("s7211 above is NULLVALUE abort\n" ));
				//abort();
				above = NULL;
			}

			if ( below && *below == JagLineSeg3DPair::NULLVALUE ) {
				#if 0
				prt(("7258 below is NULLVALUE abort\n" ));
				jarr->printKey();
				seg.println();
				//abort();
				#endif
				below = NULL;
			}

			if ( above && below ) {
				if ( above->color != below->color 
					 && line3DIntersectLine3D( above->key.x1,above->key.y1,above->key.z1,
					 						   above->key.x2,above->key.y2,above->key.z2,
									    	   below->key.x1,below->key.y1,below->key.z1,
											   below->key.x2,below->key.y2,below->key.z2 ) ) {
					prt(("s7043 rightend above below intersect\n" ));
					return true;
				}
			} else if ( above ) {
				if ( above->color != seg.color 
					 && line3DIntersectLine3D( above->key.x1,above->key.y1,above->key.z1,
					 						   above->key.x2,above->key.y2,above->key.z2,
									    	   seg.key.x1,seg.key.y1,seg.key.z1,
											   seg.key.x2,seg.key.y2,seg.key.z2 ) ) {
					prt(("s7710 rightend above seg intersect\n" ));
					return true;
				}
			} else if ( below ) {
				if ( below->color != seg.color 
					 && line3DIntersectLine3D( below->key.x1,below->key.y1,below->key.z1,
					 						   below->key.x2,below->key.y2,below->key.z2,
									    	   seg.key.x1,seg.key.y1,seg.key.z1,
											   seg.key.x2,seg.key.y2,seg.key.z2) ) {
					prt(("s4740 rightend below seg intersect\n" ));
					return true;
				}
			}
			jarr->remove( seg );
		}
	}

	delete [] points;
	delete jarr;

	prt(("s7810 no intersect\n"));
	return false;
}

bool JagGeo::polygon3DIntersectBox(  const AbaxDataString &mk1, const JagStrSplit &sp1,
									double x0, double y0, double z0,
									double w, double d, double h, 
									double nx, double ny, bool strict )
{
	// prt(("s6752 polygon3DIntersectBox ..\n" ));
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
		/***
		prt(("s6753 sp1: bbx=%f bby=%f bbz=%f    brx=%f bry=%f brz=%f\n",
				bbx, bby, bbz, brx, bry, brz ));
		prt(("s6753 x0=%f y0=%f z0=%f w=%f d=%f h=%f\n", x0, y0, z0,  w, d, h ));
		***/

        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  w, d, h ) ) {
			prt(("s6750 bound3DDisjoint return false\n" ));
            return false;
        }
        start = 1;
    }

	JagRectangle3D rect[6];
	surfacesOfBox( w,d,h, rect); // local rectangles
	for ( int i=0; i < 6; ++i ) {
		rect[i].transform( x0, y0, z0, nx, ny ); // global 
	}

    double dx1, dy1, dz1, dx2, dy2, dz2;
	JagLine3D line3d;
    const char *str;
    char *p; int i;
    for ( i=start; i < sp1.length()-1; ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx1, dy1, dz1 );
		if ( point3DWithinBox( dx1, dy1, dz1,  x0, y0, z0, w,d,h,nx,ny, strict ) ) return true;
        str = sp1[i+1].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx2, dy2, dz2 );
		line3d.x1=dx1; line3d.y1=dy1; line3d.z1=dz1;
		line3d.x2=dx2; line3d.y2=dy2; line3d.z2=dz2;
		for ( int j=0; j<6; ++j ) {
			if ( line3DIntersectRectangle3D( line3d, rect[i] ) ) return true;
		}
    }

    str = sp1[i].c_str();
    if ( strchrnum( str, ':') >= 2 ) {
        get3double(str, p, ':', dx1, dy1, dz1 );
		if ( point3DWithinBox( dx1, dy1, dz1,  x0, y0, z0, w,d,h, nx,ny, strict ) ) return true;
	}

	prt(("s6750 bound3DDisjoint return false\n" ));
	return false;
}

bool JagGeo::polygon3DIntersectSphere(  const AbaxDataString &mk1, const JagStrSplit &sp1,
 									  double x0, double y0, double z0, double r, bool strict )
{
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  r, r, r ) ) {
            return false;
        }
        start = 1;
    }

	//prt(("s9381 polygon3DIntersectSphere sp1:\n" ));
	//sp1.print();

    double dx1, dy1, dz1, dx2, dy2, dz2;
    const char *str;
    char *p; int i;
    for ( i=start; i < sp1.length()-1; ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx1, dy1, dz1 );
		if ( point3DWithinSphere( dx1, dy1, dz1,  x0, y0, z0, r, strict ) ) return true;
        str = sp1[i+1].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx2, dy2, dz2 );
		if ( line3DIntersectSphere(  dx1,dy1,dz1,dx2,dy2,dz2, x0, y0, z0, r, strict ) ) return true;
    }

    str = sp1[i].c_str();
    if ( strchrnum( str, ':') >= 2 ) {
        get3double(str, p, ':', dx1, dy1, dz1 );
		if ( point3DWithinSphere( dx1, dy1, dz1,  x0, y0, z0, r, strict ) ) return true;
	}

	return false;
}


bool JagGeo::polygon3DIntersectEllipsoid(  const AbaxDataString &mk1, const JagStrSplit &sp1,
								 double x0, double y0, double z0,
								double w, double d, double h, double nx, double ny, bool strict )
{
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  w, d, h ) ) {
            return false;
        }
        start = 1;
    }

    double dx1, dy1, dz1, dx2, dy2, dz2;
    const char *str;
    char *p; int i;
    for ( i=start; i < sp1.length()-1; ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx1, dy1, dz1 );
		if ( point3DWithinEllipsoid( dx1, dy1, dz1,  x0, y0, z0, w,d,h,nx,ny, strict ) ) return true;
        str = sp1[i+1].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx2, dy2, dz2 );
		if ( line3DIntersectEllipsoid(  dx1,dy1,dz1,dx2,dy2,dz2, x0, y0, z0, w,d,h,nx,ny, strict ) ) return true;
    }

    str = sp1[i].c_str();
    if ( strchrnum( str, ':') >= 2 ) {
        get3double(str, p, ':', dx1, dy1, dz1 );
		if ( point3DWithinEllipsoid( dx1, dy1, dz1,  x0, y0, z0, w,d,h,nx,ny, strict ) ) return true;
	}

	return false;
}

bool JagGeo::polygon3DIntersectCone(  const AbaxDataString &mk1, const JagStrSplit &sp1,
								 double x0, double y0, double z0,
								double r, double h, double nx, double ny, bool strict )
{
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  r, r, h ) ) {
            return false;
        }
        start = 1;
    }

    double dx1, dy1, dz1, dx2, dy2, dz2;
    const char *str;
    char *p; int i;
    for ( int i=start; i < sp1.length()-1; ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx1, dy1, dz1 );
		if ( point3DWithinCone( dx1, dy1, dz1,  x0, y0, z0, r,h,nx,ny, strict ) ) return true;
        str = sp1[i+1].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx2, dy2, dz2 );
		if ( line3DIntersectCone(  dx1,dy1,dz1,dx2,dy2,dz2, x0, y0, z0, r,h, nx,ny, strict ) ) return true;
    }

    str = sp1[i].c_str();
    if ( strchrnum( str, ':') >= 2 ) {
        get3double(str, p, ':', dx1, dy1, dz1 );
		if ( point3DWithinCone( dx1, dy1, dz1,  x0, y0, z0, r,h,nx,ny, strict ) ) return true;
	}
	return false;
}

bool JagGeo::polygon3DIntersectCylinder(  const AbaxDataString &mk1, const JagStrSplit &sp1,
								 double x0, double y0, double z0,
								double a, double b, double c, double nx, double ny, bool strict )
{
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        double bbx, bby, bbz, brx, bry, brz;
        boundingBox3DRegion(sp1[0], bbx, bby, bbz, brx, bry, brz );
        if ( bound3DDisjoint( bbx, bby, bbz, brx, bry, brz,  x0, y0, z0,  a, b, c ) ) {
            return false;
        }
        start = 1;
    }

    double dx1, dy1, dz1, dx2, dy2, dz2;
    const char *str;
    char *p; int i;
    for ( i=start; i < sp1.length()-1; ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx1, dy1, dz1 );
		if ( point3DWithinCylinder( dx1, dy1, dz1,  x0, y0, z0, a,c,nx,ny, strict ) ) return true;
        str = sp1[i+1].c_str();
        if ( strchrnum( str, ':') < 2 ) continue;
        get3double(str, p, ':', dx2, dy2, dz2 );
		if ( line3DIntersectCylinder(  dx1,dy1,dz1,dx2,dy2,dz2, x0, y0, z0, a,b,c, nx,ny, strict ) ) return true;
    }

    str = sp1[i].c_str();
    if ( strchrnum( str, ':') >= 2 ) {
        get3double(str, p, ':', dx1, dy1, dz1 );
		if ( point3DWithinCylinder( dx1, dy1, dz1,  x0, y0, z0, a,c,nx,ny, strict ) ) return true;
	}
	return false;
}


// 2D linestring
bool JagGeo::doLineStringIntersect( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, 
								    const AbaxDataString &mk2, const AbaxDataString &colType2, 
								    int srid2, const JagStrSplit &sp2, bool strict )
{
	// like point within
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return lineStringIntersectTriangle( mk1, sp1, x1, y1, x2, y2, x3, y3, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING ) {
		return lineStringIntersectLineString( mk1, sp1, mk2, sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3 );
		return lineStringIntersectRectangle( mk1, sp1, x0, y0, a,a, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		return lineStringIntersectRectangle( mk1, sp1, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3 );
		return lineStringIntersectEllipse( mk1, sp1, x0, y0, r,r, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4 );
		return lineStringIntersectEllipse( mk1, sp1, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return polygonIntersectLineString( mk2, sp2, mk1, sp1 );
	}
	return false;
}

bool JagGeo::doLineString3DIntersect( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, 
									  const AbaxDataString &mk2, const AbaxDataString &colType2, 
									  int srid2, const JagStrSplit &sp2, bool strict )
{
	prt(("s8761 doLineString3DIntersect colType2=[%s]\n", colType2.c_str() ));
	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		prt(("s0872 lineString3DIntersectBox ..\n" ));
		return lineString3DIntersectBox( mk1, sp1, x0, y0, z0, r,r,r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return lineString3DIntersectLineString3D( mk1, sp1, mk2, sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return lineString3DIntersectBox( mk1, sp1, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return lineString3DIntersectSphere( mk1, sp1, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return lineString3DIntersectEllipsoid( mk1, sp1, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return lineString3DIntersectCone( mk1, sp1, x0, y0, z0, r,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CYLINDER ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return lineString3DIntersectCylinder( mk1, sp1, x0, y0, z0, r,r,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_TRIANGLE3D ) {
		return lineString3DIntersectTriangle3D( mk1, sp1, mk2, sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE3D ) {
		return lineString3DIntersectSquare3D( mk1, sp1, mk2, sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE3D ) {
		return lineString3DIntersectRectangle3D( mk1, sp1, mk2, sp2 );
	}
	return false;
}

double JagGeo::doPolygonArea( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1 )
{
	int start = 0;
	if ( mk1 == JAG_OJAG ) { start = 1; }
	double dx, dy;
	const char *str;
	char *p;
	double ar, area = 0.0;
	int ring = 0; // n-th ring in polygon
	if ( 0 == srid1 ) {
    	JagVector<std::pair<double,double>> vec;
    	for ( int i=start; i < sp1.length(); ++i ) {
    		if ( sp1[i] == "!" ) break; // should not happen
    		if ( sp1[i] == "|" ) {
    			ar = computePolygonArea( vec );
				if ( 0 == ring ) {
    				area = ar;
				} else {
    				area -= ar;
				}
				vec.clean();
				++ring;
			}
    		str = sp1[i].c_str();
    		if ( strchrnum( str, ':') < 1 ) continue;
    		get2double(str, p, ':', dx, dy );
    		vec.append( std::make_pair(dx,dy) );
    	}

		if ( vec.size() > 0 ) {
    		ar = computePolygonArea( vec );
			if ( 0 == ring ) {
    			area = ar;
			} else {
    			area -= ar;
			}
		}
	} else if ( JAG_GEO_WGS84 == srid1 ) {
		const Geodesic& geod = Geodesic::WGS84();
		PolygonArea poly(geod);
		int numPoints = 0;
		double perim;
    	for ( int i=start; i < sp1.length(); ++i ) {
    		if ( sp1[i] == "!" ) break;
    		if ( sp1[i] == "|" ) {
				poly.Compute( false, true, perim, ar );
				if ( 0 == ring ) {
    				area = ar;
				} else {
    				area -= ar;
				}
				poly.Clear();
				numPoints = 0;
			} 
    		str = sp1[i].c_str();
    		if ( strchrnum( str, ':') < 1 ) continue;
    		get2double(str, p, ':', dx, dy );
			poly.AddPoint( dx, dy );
			++numPoints;
    	}

		if ( numPoints > 0 ) {
			poly.Compute( false, true, perim, ar );
			if ( 0 == ring ) {
    			area = ar;
			} else {
    			area -= ar;
			}
		}
	} 
	return area;
}

double JagGeo::computePolygonArea( const JagVector<std::pair<double,double>> &vec )
{
	int n = vec.size();
	double x[n+2];
	double y[n+2];
	for ( int i=0; i < n; ++i ) {
		x[i+1] = vec[i].first;
		y[i+1] = vec[i].second;
	}

    // e.g. n=3:
	// y0 y1 y2 y3 y4
	//    22 33 72 
	y[0] = y[n];
	y[n+1] = y[1];
	double area = 0.0;
	for ( int i = 1; i <= n; ++i ) {
		area += fabs( x[i] * ( y[i+1] - y[i-1]) )/2.0;
	}
	return area;
}

bool JagGeo::doPolygonIntersect( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, 
								    const AbaxDataString &mk2, const AbaxDataString &colType2, 
								    int srid2, const JagStrSplit &sp2, bool strict )
{
	//prt(("s2268 doPolygonIntersect ...\n" ));

	// like point within
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return polygonIntersectTriangle( mk1, sp1, x1, y1, x2, y2, x3, y3, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_LINE ) {
		double x1 = jagatof( sp2[0].c_str() ); 
		double y1 = jagatof( sp2[1].c_str() ); 
		double x2 = jagatof( sp2[2].c_str() ); 
		double y2 = jagatof( sp2[3].c_str() ); 
		return polygonIntersectLine( mk1, sp1, x1, y1, x2, y2 );
	} else if ( colType2 == JAG_C_COL_TYPE_POINT ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		//prt(("s2938 pointWithinPolygon x=%.2f y=%.2f\n", x, y ));
		//sp1.print();
		return pointWithinPolygon( x, y, mk1, sp1, false );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING ) {
		return polygonIntersectLineString( mk1, sp1, mk2, sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3 );
		return polygonIntersectRectangle( mk1, sp1, x0, y0, a,a, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		return polygonIntersectRectangle( mk1, sp1, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3 );
		return polygonIntersectEllipse( mk1, sp1, x0, y0, r,r, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4 );
		return polygonIntersectEllipse( mk1, sp1, x0, y0, a, b, nx, strict );
	}
	return false;
}

bool JagGeo::doPolygon3DIntersect( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, 
									  const AbaxDataString &mk2, const AbaxDataString &colType2, 
									  int srid2, const JagStrSplit &sp2, bool strict )
{
	prt(("s8761 dopolygon3DIntersect colType2=[%s]\n", colType2.c_str() ));
	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		prt(("s0872 polygon3DIntersectBox ..\n" ));
		return polygon3DIntersectBox( mk1, sp1, x0, y0, z0, r,r,r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return polygon3DIntersectLineString3D( mk1, sp1, mk2, sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return polygon3DIntersectBox( mk1, sp1, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return polygon3DIntersectSphere( mk1, sp1, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return polygon3DIntersectEllipsoid( mk1, sp1, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return polygon3DIntersectCone( mk1, sp1, x0, y0, z0, r,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CYLINDER ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return polygon3DIntersectCylinder( mk1, sp1, x0, y0, z0, r,r,h, nx,ny, strict );
	}
	return false;
}

bool JagGeo::doMultiPolygonIntersect( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, 
								    const AbaxDataString &mk2, const AbaxDataString &colType2, 
								    int srid2, const JagStrSplit &sp2, bool strict )
{
	// like point within
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return polygonIntersectTriangle( mk1, sp1, x1, y1, x2, y2, x3, y3, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING ) {
		return polygonIntersectLineString( mk1, sp1, mk2, sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3 );
		return polygonIntersectRectangle( mk1, sp1, x0, y0, a,a, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		return polygonIntersectRectangle( mk1, sp1, x0, y0, a, b, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3 );
		return polygonIntersectEllipse( mk1, sp1, x0, y0, r,r, nx, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4 );
		return polygonIntersectEllipse( mk1, sp1, x0, y0, a, b, nx, strict );
	}
	return false;
}

bool JagGeo::doMultiPolygon3DIntersect( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, 
									  const AbaxDataString &mk2, const AbaxDataString &colType2, 
									  int srid2, const JagStrSplit &sp2, bool strict )
{
	prt(("s8761 domultiPolygon3DIntersect colType2=[%s]\n", colType2.c_str() ));
	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		prt(("s0872 multiPolygon3DIntersectBox ..\n" ));
		return polygon3DIntersectBox( mk1, sp1, x0, y0, z0, r,r,r, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return polygon3DIntersectLineString3D( mk1, sp1, mk2, sp2 );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return polygon3DIntersectBox( mk1, sp1, x0, y0, z0, w,d,h, nx, ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return polygon3DIntersectSphere( mk1, sp1, x, y, z, r, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return polygon3DIntersectEllipsoid( mk1, sp1, x0, y0, z0, w,d,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return polygon3DIntersectCone( mk1, sp1, x0, y0, z0, r,h, nx,ny, strict );
	} else if ( colType2 == JAG_C_COL_TYPE_CYLINDER ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return polygon3DIntersectCylinder( mk1, sp1, x0, y0, z0, r,r,h, nx,ny, strict );
	}
	return false;
}

///////////////////////////////////// triangle 3D /////////////////////////////////
bool JagGeo::triangle3DIntersectBox(  double x10, double y10, double z10, 
								   double x20, double y20, double z20, 
									double x30, double y30, double z30,
									double x0, double y0, double z0,
									double w, double d, double h, 
									double nx, double ny, bool strict )
{
	if ( ! validDirection(nx, ny) ) return false;
	double mxd = jagMax(w,d,h);
	if ( jagMax(x10, x20, x30) < x0-mxd ) return false;
	if ( jagMax(y10, y20, y30) < y0-mxd ) return false;
	if ( jagMax(z10, z20, z30) < z0-mxd ) return false;
	if ( jagMin(x10, x20, x30) > x0+mxd ) return false;
	if ( jagMin(y10, y20, y30) > y0+mxd ) return false;
	if ( jagMin(z10, z20, z30) > z0+mxd ) return false;

	if ( triangle3DWithinBox( x10,y10,z10, x20,y20,z20,  x30,y30,z30, x0,y0,z0, w,d,h, nx,ny, false ) ) {
		return true;
	}

	JagLine3D line[3];
	edgesOfTriangle3D( x10,y10,z10,x20,y20,z20, x30,y30,z30, line );
	for ( int i=0; i <3; ++i ) {
		if ( line3DIntersectBox( line[i], x0, y0, z0, w,d,h, nx, ny, false ) ) {
			return true;
		}
	}

	// box inside triangle
	// triangle as base plane; check z- coords of all box corners points. if all same side, then disjoint
	JagPoint3D corn[8];
	double nx0, ny0;
	triangle3DNormal(  x10,y10,z10, x20,y20,z20,  x30,y30,z30, nx0, ny0 );
	int up=0, down=0;
	for ( int i=0; i <8; ++i ) {
		corn[i].transform( x10, y10, z10, nx0, ny0 );
		if ( jagGE(corn[i].z, 0.0 ) ) ++up;
		else ++down;
	}
	if ( 0==up || 0==down ) return false;
	return true;
}


bool JagGeo::triangle3DIntersectEllipsoid(  double x10, double y10, double z10, double x20, double y20, double z20,
										double x30, double y30, double z30,
								 double x0, double y0, double z0,
								double w, double d, double h, double nx, double ny, bool strict )
{
	double mxd = jagMax(w,d,h);
	if ( jagMax(x10, x20, x30) < x0-mxd ) return false;
	if ( jagMax(y10, y20, y30) < y0-mxd ) return false;
	if ( jagMax(z10, z20, z30) < z0-mxd ) return false;
	if ( jagMin(x10, x20, x30) > x0+mxd ) return false;
	if ( jagMin(y10, y20, y30) > y0+mxd ) return false;
	if ( jagMin(z10, z20, z30) > z0+mxd ) return false;

	if ( triangle3DWithinEllipsoid( x10,y10,z10, x20,y20,z20,  x30,y30,z30, x0,y0,z0, w,d,h, nx,ny, false ) ) {
		return true;
	}

	// each edge of tri intersect ellipsoid
	JagLine3D line[3];
	edgesOfTriangle3D( x10,y10,z10,x20,y20,z20, x30,y30,z30, line );
	for ( int i=0; i <3; ++i ) {
		if ( line3DIntersectEllipsoid( line[i].x1, line[i].y1, line[i].z1, line[i].x2, line[i].y2, line[i].z2, 
										x0, y0, z0, w,d,h, nx, ny, false ) ) {
			return true;
		}
	}

	// ellipsoid inside triangle: surface sect
	JagPoint3D point[3];
	pointsOfTriangle3D(  x10,y10,z10,x20,y20,z20, x30,y30,z30, point );
	for ( int i=0; i <3; ++i ) {
		point[i].transform(x0,y0,z0, nx,ny  );
		// ellipsoid as normal
	}
	double A, B, C, D;
	triangle3DABCD( point[0].x, point[0].y, point[0].z,
					point[1].x, point[1].y, point[1].z,
					point[2].x, point[2].y, point[2].z, A, B, C, D );
	if ( planeIntersectNormalEllipsoid( A, B, C, D, w,d,h ) ) {
		return true;
	}

	return false;
}


bool JagGeo::triangle3DIntersectCone(  double x10, double y10, double z10, double x20, double y20, double z20,
										double x30, double y30, double z30,
								 double x0, double y0, double z0,
								double r, double h, double nx, double ny, bool strict )
{
	double mxd = jagmax(r,h);
	if ( jagMax(x10, x20, x30) < x0-mxd ) return false;
	if ( jagMax(y10, y20, y30) < y0-mxd ) return false;
	if ( jagMax(z10, z20, z30) < z0-mxd ) return false;
	if ( jagMin(x10, x20, x30) > x0+mxd ) return false;
	if ( jagMin(y10, y20, y30) > y0+mxd ) return false;
	if ( jagMin(z10, z20, z30) > z0+mxd ) return false;

	if ( triangle3DWithinCone( x10,y10,z10, x20,y20,z20,  x30,y30,z30, x0,y0,z0, r,h, nx,ny, false ) ) {
		return true;
	}

	// each edge of tri intersect ellipsoid
	JagLine3D line[3];
	edgesOfTriangle3D( x10,y10,z10,x20,y20,z20, x30,y30,z30, line );
	for ( int i=0; i <3; ++i ) {
		if ( line3DIntersectCone( line[i].x1, line[i].y1, line[i].z1, line[i].x2, line[i].y2, line[i].z2, 
										x0, y0, z0, r,h, nx, ny, false ) ) {
			return true;
		}
	}

	JagPoint3D point[3];
	pointsOfTriangle3D(  x10,y10,z10,x20,y20,z20, x30,y30,z30, point );
	for ( int i=0; i <3; ++i ) {
		point[i].transform(x0,y0,z0, nx,ny  );
		// ellipsoid as normal
	}
	double A, B, C, D;
	triangle3DABCD( point[0].x, point[0].y, point[0].z, point[1].x, point[1].y, point[1].z,
					point[2].x, point[2].y, point[2].z, A, B, C, D );
	if ( planeIntersectNormalCone( A, B, C, D, r,  h ) ) {
		return true;
	}

	return false;
}

/////////////////////////////////////////////// 3D box intersection
bool JagGeo::boxIntersectBox(  double px0, double py0, double pz0, double a0, double b0, double c0,
								double nx0, double ny0,
						        double x, double y, double z, 
							    double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if ( bound3DDisjoint( px0,py0,pz0, a0,b0,c0,  x,y,z, w,d,h ) ) return false;
	if ( point3DWithinBox( px0,py0,pz0, x, y, z, w,d,h, nx, ny, false ) ) { return true; }
	if ( point3DWithinBox( x,y,z, px0,py0,pz0, a0,b0,c0, nx0, ny0, false ) ) { return true; }

	// 8 corners mutual check
	JagPoint3D corn1[8];
	cornersOfBox( a0,b0,c0, corn1 );
	for ( int i=0; i < 8; ++i ) {
		corn1[i].transform(px0,py0,pz0, nx0, ny0 );
	}

	JagPoint3D corn2[8];
	cornersOfBox( x,y,z, corn2 );
	for ( int i=0; i < 8; ++i ) {
		corn2[i].transform(x,y,z, nx, ny);
	}

	for ( int i=0; i < 8; ++i ) {
		if ( point3DWithinBox( corn1[i].x, corn1[i].y, corn1[i].z, x, y, z, w,d,h, nx, ny, false ) ) {
			return true;
		}
	}

	for ( int i=0; i < 8; ++i ) {
		if ( point3DWithinBox( corn2[i].x, corn2[i].y, corn2[i].z, px0,py0,pz0, a0,b0,c0, nx0, ny0, false ) ) {
			return true;
		}
	}

	// corners of first box on both sides of box2?
	int up=0, down=0;
	for ( int i=0; i < 8; ++i ) {
		corn1[i].transform(x,y,z, nx, ny );
		if ( jagEQ(corn1[i].z, 0.0) ) return true;
		if ( corn1[i].z > 0.0 ) ++up;
		else ++down;
	}
	if ( up *down != 0) return true;
	return false;
}

bool JagGeo::boxIntersectEllipsoid(  double px0, double py0, double pz0, double a0, double b0, double c0,
								  double nx0, double ny0,
						        	double x, double y, double z, 
							    	double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;

	if ( bound3DDisjoint( px0,py0,pz0, a0,b0,c0,  x,y,z, w,d,h ) ) return false;
	if ( point3DWithinEllipsoid( px0,py0,pz0, x, y, z, w,d,h, nx, ny, false ) ) { return true; }
	if ( point3DWithinBox( x,y,z, px0,py0,pz0, a0,b0,c0, nx0, ny0, false ) ) { return true; }

	// 8 corners of box inside ellipsoid
	JagPoint3D corn1[8];
	cornersOfBox( a0,b0,c0, corn1 );
	for ( int i=0; i < 8; ++i ) {
		corn1[i].transform(px0,py0,pz0, nx0, ny0 );
		if ( point3DWithinEllipsoid( corn1[i].x,  corn1[i].y, corn1[i].z, x, y, z, w,d,h, nx, ny, false ) ) { 
			return true; 
		}
	}

	// 12 edges intersect ellipsoid
	int num = 12;
	JagLine3D line[num];
	edgesOfBox( w, d, h, line );
	transform3DLinesLocal2Global( px0, py0, pz0, nx0, ny0, num, line );  // to x-y-z
	for ( int i=0; i<num; ++i ) {
		if ( line3DIntersectEllipse3D( line[i], x,y,z, w,h, nx, ny )  ) {
			return true;
		}
	}

	// 6 planes intersect ellipsoid
	JagTriangle3D tri[6];
	triangleSurfacesOfBox(w, d, h, tri );
   	double A, B, C, D;
	for ( int i=0; i < 6; ++i ) {
    	tri[i].transform(x,y,z, nx,ny  );
    	triangle3DABCD( tri[i].x1, tri[i].y1, tri[i].z1,
						tri[i].x2, tri[i].y2, tri[i].z2,
						tri[i].x3, tri[i].y3, tri[i].z3,
    					A, B, C, D );
    	if ( planeIntersectNormalEllipsoid( A, B, C, D, w,d,h ) ) {
    		return true;
    	}
	}

	// corners of first box on both sides of box2?
	int up=0, down=0;
	for ( int i=0; i < 8; ++i ) {
		corn1[i].transform(x,y,z, nx, ny );
		if ( jagEQ( corn1[i].z, 0.0 ) ) return true;
		if ( corn1[i].z > 0.0 ) ++up;
		else ++down;
	}
	if ( up *down != 0) return true;
	return false;
}

bool JagGeo::boxIntersectCone(  double px0, double py0, double pz0, double a0, double b0, double c0,
								  double nx0, double ny0,
						        	double x0, double y0, double z0, 
							    	double r, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;

	if ( bound3DDisjoint( px0,py0,pz0, a0,b0,c0,  x0,y0,z0, r,r,h ) ) return false;
	if ( point3DWithinCone( px0,py0,pz0, x0, y0, z0, r,h, nx, ny, false ) ) { return true; }
	if ( point3DWithinBox( x0,y0,z0, px0,py0,pz0, a0,b0,c0, nx0, ny0, false ) ) { return true; }

	//if ( boxWithinCone( px0,py0,pz0, a0,b0,c0, nx0, ny0, x0, y0, z0, r,h, nx, ny, false ) ) { return true; }
	//if ( coneWithinBox(  x0,y0,z0, r, h, nx, ny, px0,py0,pz0, a0,b0,c0, nx0, ny0, false ) ) return true;


	// triangle of the box
	JagTriangle3D tri[6];
	triangleSurfacesOfBox(a0, b0, c0, tri );
   	double A, B, C, D;
	for ( int i=0; i < 6; ++i ) {
    	tri[i].transform(x0,y0,z0, nx,ny  );
    	triangle3DABCD( tri[i].x1, tri[i].y1, tri[i].z1,
						tri[i].x2, tri[i].y2, tri[i].z2,
						tri[i].x3, tri[i].y3, tri[i].z3,
    					A, B, C, D );
    	if ( planeIntersectNormalCone( A, B, C, D, r,h ) ) {
    		return true;
    	}
	}
	return false;
}

bool JagGeo::ellipsoidIntersectBox(  double px0, double py0, double pz0, double a0, double b0, double c0,
								double nx0, double ny0,
						        double x0, double y0, double z0, 
							    double w, double d, double h, double nx, double ny, bool strict )
{
	return boxIntersectEllipsoid( x0, y0, z0, w,d,h, nx, ny, px0,py0,pz0, a0,b0,c0, nx0, ny0, false );
}

bool JagGeo::ellipsoidIntersectEllipsoid(  double px0, double py0, double pz0, double a0, double b0, double c0,
								  double nx0, double ny0,
						        	double x0, double y0, double z0, 
							    	double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if ( bound3DDisjoint( px0,py0,pz0, a0,b0,c0,  x0,y0,z0, w,d,h ) ) return false;
	if ( point3DWithinEllipsoid( px0,py0,pz0, x0, y0, z0, w,d,h, nx, ny, false ) ) { return true; }
	if ( point3DWithinEllipsoid( x0,y0,z0, px0, py0, pz0, a0,b0,c0, nx0, ny0, false ) ) { return true; }
	//if ( ellipsoidWithinEllipsoid( px0,py0,pz0, a0,b0,c0,nx0,ny0,  x0,y0,z0, w,d,h, nx,ny, false ) ) return true;
	//if ( ellipsoidWithinEllipsoid( x0,y0,z0, w,d,h, nx,ny,  px0,py0,pz0, a0,b0,c0,nx0,ny0,  false ) ) return true;


	JagVector<JagPoint3D> vec;
	double loc_x, loc_y, loc_z;
	samplesOnEllipsoid( px0, py0, pz0, a0, b0, c0, nx0, ny0, NUM_SAMPLE, vec );
	for ( int i=0; i <vec.size(); ++i ) {
    	transform3DCoordGlobal2Local( x0, y0, z0, vec[i].x, vec[i].y, vec[i].z, nx, ny, loc_x, loc_y, loc_z );
		if ( point3DWithinNormalEllipsoid( loc_x, loc_y, loc_z, w,d,h, strict ) ) { return true; }
	}
	return false;
}

bool JagGeo::ellipsoidIntersectCone(  double px0, double py0, double pz0, double a0, double b0, double c0,
								  double nx0, double ny0,
						        	double x0, double y0, double z0, 
							    	double r, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;

	if ( bound3DDisjoint( px0,py0,pz0, a0,b0,c0,  x0,y0,z0, r,r,h ) ) return false;

	if ( point3DWithinCone( px0,py0,pz0, x0, y0, z0, r,h, nx, ny, false ) ) { return true; }
	if ( point3DWithinEllipsoid( x0,y0,z0, px0, py0, pz0, a0,b0,c0, nx0, ny0, false ) ) { return true; }

	//if ( ellipsoidWithinCone( px0,py0,pz0, a0,b0,c0,nx0,ny0,  x0,y0,z0, r,h, nx,ny, false ) ) return true;
	//if ( coneWithinEllipsoid( x0,y0,z0, r,h, nx,ny,  px0,py0,pz0, a0,b0,c0,nx0,ny0,  false ) ) return true;


	JagVector<JagPoint3D> vec;
	double loc_x, loc_y, loc_z;
	samplesOnEllipsoid( px0, py0, pz0, a0, b0, c0, nx0, ny0, NUM_SAMPLE, vec );
	for ( int i=0; i <vec.size(); ++i ) {
		if ( point3DWithinCone( vec[i].x, vec[i].y, vec[i].z, x0,y0,z0, r, h, nx, ny, strict ) ) {
			return true;
		}
	}

	// sample lines in cone
	JagVector<JagLine3D> vec2;
	sampleLinesOnCone( x0, y0, z0, r, h, nx, ny, NUM_SAMPLE, vec2 ); 
	for ( int i=0; i < vec2.size(); ++i ) {
		if ( line3DIntersectEllipsoid( vec2[i].x1, vec2[i].y1, vec2[i].z1, vec2[i].x2, vec2[i].y2, vec2[i].z2,
										px0, py0, pz0, a0, b0, c0, nx0, ny0, false ) ) return true;
	}

	return false;
}


///////////////////////////////////// 3D cylinder /////////////////////////////////
bool JagGeo::cylinderIntersectBox(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
						        double x, double y, double z, 
							    double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;

	if ( bound3DDisjoint( px0,py0,pz0, pr0,pr0,c0,  x,y,z, w,d,h ) ) return false;

	if ( point3DWithinBox( px0,py0,pz0,   x, y, z, w,d,h, nx, ny, false ) ) { return true; }
	if ( point3DWithinCylinder( x,y,z, px0, py0, pz0, pr0,c0, nx0, ny0, false ) ) { return true; }

	//if ( cylinderIntersectBox( px0,py0,pz0, a0,b0,c0,nx0,ny0,  x,y,z, w,d,h, nx,ny, false ) ) return true;
	//if ( boxWithinCylinder( x,y,z, w,d,h, nx,ny,  px0,py0,pz0, pr0,pr0,c0,nx0,ny0,  false ) ) return true;

	// bx corners in cylinder
	// 8 corners check
	JagPoint3D corn[8];
	cornersOfBox( w,d,h, corn );
	for ( int i=0; i < 8; ++i ) {
		corn[i].transform(px0,py0,pz0, nx0, ny0 );
	}
	for ( int i=0; i < 8; ++i ) {
		if ( point3DWithinCylinder( corn[i].x, corn[i].y, corn[i].z, px0, py0, pz0, pr0,c0, nx0, ny0, false ) ) {
			return true;
		}
	}

	// samples lines of cylinder intersect box 6 planes
	JagVector<JagLine3D> vec;
	sampleLinesOnCylinder( px0, py0, pz0, pr0, c0, nx0, ny0, NUM_SAMPLE, vec );
	JagRectangle3D rect[6];
	surfacesOfBox(w, d, h, rect );
	for ( int i=0; i<6; ++i ) {
		rect[i].transform( x,y,z, nx, ny );
	}

	for ( int i=0; i < vec.size(); ++i ) {
		for ( int j=0; j < 6; ++j ) {
			if ( line3DIntersectRectangle3D( vec[i], rect[j] ) ) return true;
		}
	}
	return false;
}

bool JagGeo::cylinderIntersectEllipsoid(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
						        	double x, double y, double z, 
							    	double w, double d, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;
	if ( bound3DDisjoint( px0,py0,pz0, pr0,pr0,c0,  x,y,z, w,d,h ) ) return false;

	if ( point3DWithinBox( px0,py0,pz0,   x, y, z, w,d,h, nx, ny, false ) ) { return true; }
	if ( point3DWithinCylinder( x,y,z, px0, py0, pz0, pr0,c0, nx0, ny0, false ) ) { return true; }

	// sample lines on cynlder intersect ellipsoid
	JagVector<JagLine3D> vec;
	sampleLinesOnCylinder( px0, py0, pz0, pr0, c0, nx0, ny0, NUM_SAMPLE, vec );
	for ( int i=0; i < vec.size(); ++i ) {
		for ( int j=0; j < 6; ++j ) {
			if ( line3DIntersectEllipsoid( vec[i].x1, vec[i].y1, vec[i].z1, vec[i].x2, vec[i].y2, vec[i].z2, 
										   x,y,z,w,d,h,nx,ny, false ) ) return true;
		}
	}

	// samples points on ellipsoid witin cylinder
	JagVector<JagPoint3D> vec2;
	samplesOnEllipsoid( x, y, z, w, d, h, nx, ny, NUM_SAMPLE, vec2 );
	for ( int i=0; i <vec2.size(); ++i ) {
		if ( point3DWithinCylinder( vec2[i].x, vec2[i].y, vec2[i].z, px0,py0,pz0, pr0,c0,nx0,ny0,false) ) {
			return true;
		}
	}
	return false;
}

bool JagGeo::cylinderIntersectCone(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
						        	double x, double y, double z, 
							    	double r, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;

	if ( bound3DDisjoint( px0,py0,pz0, pr0,pr0,c0,  x,y,z, r,r,h ) ) return false;
	if ( point3DWithinCone( px0,py0,pz0,   x, y, z, r,h, nx, ny, false ) ) { return true; }
	if ( point3DWithinCylinder( x,y,z, px0, py0, pz0, pr0,c0, nx0, ny0, false ) ) { return true; }

	// sample lines on cynlder intersect cone
	JagVector<JagLine3D> vec;
	sampleLinesOnCylinder( px0, py0, pz0, pr0, c0, nx0, ny0, NUM_SAMPLE, vec );
	for ( int i=0; i < vec.size(); ++i ) {
		for ( int j=0; j < 6; ++j ) {
			if ( line3DIntersectCone( vec[i].x1, vec[i].y1, vec[i].z1, vec[i].x2, vec[i].y2, vec[i].z2, 
										   x,y,z,r,h,nx,ny, false ) ) return true;
		}
	}

	// samples points on ellipsoid witin cylinder
	JagVector<JagLine3D> vec2;
	sampleLinesOnCone( x, y, z, r, h, nx, ny, NUM_SAMPLE, vec2 );
	for ( int i=0; i <vec2.size(); ++i ) {
		if ( line3DIntersectCylinder( vec2[i].x1, vec2[i].y1, vec2[i].z1,
									vec2[i].x2, vec2[i].y2, vec2[i].z2,
									px0,py0,pz0, pr0,pr0,c0,nx0,ny0,false) ) {
			return true;
		}
	}
	return false;
}


///////////////////////////////////// 3D cone /////////////////////////////////
bool JagGeo::coneIntersectBox(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
						        double x, double y, double z, 
							    double w, double d, double h, double nx, double ny, bool strict )
{
	return boxIntersectCone( x,y,z,w,d,h,nx,ny,  px0,py0,pz0,pr0,c0,nx0,ny0,false);
}

bool JagGeo::coneIntersectEllipsoid(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
						        	double x, double y, double z, 
							    	double w, double d, double h, double nx, double ny, bool strict )
{
	return ellipsoidIntersectCone( x,y,z,w,d,h,nx,ny, px0,py0,pz0,pr0,c0,nx0,ny0,false);
}


bool JagGeo::coneIntersectCone(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
						        	double x, double y, double z, 
							    	double r, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;

	if ( bound3DDisjoint( px0,py0,pz0, pr0,pr0,c0,  x,y,z, r,r,h ) ) return false;
	if ( point3DWithinCone( px0,py0,pz0,   x, y, z, r,h, nx, ny, false ) ) { return true; }
	if ( point3DWithinCone( x,y,z, px0, py0, pz0, pr0,c0, nx0, ny0, false ) ) { return true; }

	// sample lines on cynlder intersect cone
	JagVector<JagLine3D> vec;
	sampleLinesOnCylinder( px0, py0, pz0, pr0, c0, nx0, ny0, NUM_SAMPLE, vec );
	for ( int i=0; i < vec.size(); ++i ) {
		for ( int j=0; j < 6; ++j ) {
			if ( line3DIntersectCylinder( vec[i].x1, vec[i].y1, vec[i].z1, vec[i].x2, vec[i].y2, vec[i].z2, 
										   x,y,z,r,r,h,nx,ny, false ) ) return true;
		}
	}

	// samples points on ellipsoid witin cylinder
	JagVector<JagLine3D> vec2;
	sampleLinesOnCylinder( x, y, z, r, h, nx, ny, NUM_SAMPLE, vec2 );
	for ( int i=0; i <vec2.size(); ++i ) {
		if ( line3DIntersectCylinder( vec2[i].x1, vec2[i].y1, vec2[i].z1,
									vec2[i].x2, vec2[i].y2, vec2[i].z2,
									px0,py0,pz0, pr0,pr0,c0,nx0,ny0,false) ) {
			return true;
		}
	}
	return false;

}


bool JagGeo::circle3DIntersectCone( double px0, double py0, double pz0, double pr0, double nx0, double ny0,
                                 double x, double y, double z,
                                 double r, double h, double nx, double ny, bool strict )
{
	if ( ! validDirection(nx0, ny0) ) return false;
	if ( ! validDirection(nx, ny) ) return false;

	if ( bound3DDisjoint( px0,py0,pz0, pr0,pr0,pr0,  x,y,z, r,r,h ) ) return false;
	if ( point3DWithinCone( px0,py0,pz0,  x, y, z, r,h, nx, ny, false ) ) { return true; }

	// sample lines of cone intesect circle
	JagVector<JagLine3D> vec2;
	sampleLinesOnCone( x, y, z, r, h, nx, ny, NUM_SAMPLE, vec2 );
	for ( int i=0; i <vec2.size(); ++i ) {
		if ( line3DIntersectEllipse3D( vec2[i], px0,py0,pz0, pr0,pr0,nx0,ny0) ) {
			return true;
		}
	}
	return false;
}

/////////////////////////////// end intersect methods //////////////////////////////////////////////


/////////////////////////////////// misc methods /////////////////////////////////////////////////////////
bool JagGeo::locIn3DCenterBox( double loc_x, double loc_y, double loc_z, double a, double b, double c, bool strict )
{
	if ( strict ) {
		if ( jagLE(loc_x, -a) ) return false;
		if ( jagLE(loc_y, -b) ) return false;
		if ( jagLE(loc_z, -c) ) return false;
		if ( jagGE(loc_x, a) ) return false;
		if ( jagGE(loc_y, b) ) return false;
		if ( jagGE(loc_z, c) ) return false;
	} else {
		if ( loc_x < -a ) return false;
		if ( loc_y < -b ) return false;
		if ( loc_z < -c ) return false;
		if ( loc_x > a ) return false;
		if ( loc_y > b ) return false;
		if ( loc_z > c ) return false;
	}

	return true;

}

bool JagGeo::locIn2DCenterBox( double loc_x, double loc_y, double a, double b, bool strict )
{
	if ( strict ) {
		if ( jagLE(loc_x, -a) ) return false;
		if ( jagLE(loc_y, -b) ) return false;
		if ( jagGE(loc_x, a) ) return false;
		if ( jagGE(loc_y, b) ) return false;
	} else {
		if ( loc_x < -a ) return false;
		if ( loc_y < -b ) return false;
		if ( loc_x > a ) return false;
		if ( loc_y > b ) return false;
	}
	return true;
}


// line1:  y = slope1 * x + c1
// line2:  y = slope2 * x + c2
bool JagGeo::twoLinesIntersection( double slope1, double c1, double slope2, double c2,
	                                      double &outx, double &outy )
{
	if ( jagEQ(slope1, slope2) ) return false;
	outx  = (c2-c1)/(slope1-slope2);
	outy = ( slope1*c2 - slope2*c1) / (slope1-slope2);
	return true;
}

// ellipse relative to origin at (0,0) nx is unit vector on x-axis of y-diretion of ellipse
// x1,y1, .... coord of 4 corners in real x-y sys
void JagGeo::orginBoundingBoxOfRotatedEllipse( double a, double b, double nx,
                                        double &x1, double &y1,
                                        double &x2, double &y2,
                                        double &x3, double &y3,
                                        double &x4, double &y4 )
{
	// if ellipse is not rotated
	if ( jagIsZero( nx ) ) {
		x1 = -a; y1 = b;
		x2 = a; y2 = b;
		x3 = a; y3 = -b;
		x4 = -a; y4 = -b;
		return;
	}
	//  tangent lines are  = m*x + c
	// nx = sin(a)
	double ny = sqrt( 1.0 - nx*nx);  // cos(a)
	double m1, c1, m2, c2, m3, c3, m4, c4;
	double a2= a*a;
	double b2= b*b;

	m1 = nx/ny;
	c1 = sqrt( a2*m1*m1 + b2);

	m2 = m1;
	c2 = -c1;  // m2 // m1

	m3 = -1.0/m1;  // m3 _|_ m1
	c3 = -sqrt( a2*m3*m3 + b2);

	m4 = m3;    // m4 // m3
	c4 = -c3;

	// twoLinesIntersection( double slope1, double c1, double slope2, double c2, double &outx, double &outy )
	twoLinesIntersection( m1, c1, m3, c3, x1, y1 );
	twoLinesIntersection( m3, c3, m2, c2, x2, y2 );
	twoLinesIntersection( m2, c2, m4, c4, x3, y3 );
	twoLinesIntersection( m4, c4, m1, c1, x4, y4 );
}


// 2D
// get x1, y1, ... relative to x-y, absolute coords
void JagGeo::boundingBoxOfRotatedEllipse( double x0, double y0, double a, double b, double nx,
                                        double &x1, double &y1,
                                        double &x2, double &y2,
                                        double &x3, double &y3,
                                        double &x4, double &y4 )
{
	// x1 y1, .... relative to ellipse center, in x -y coord
	orginBoundingBoxOfRotatedEllipse( a, b, nx, x1, y1, x2, y2, x3, y3, x4, y4 );
	x1 += x0; y1 += y0;
	x2 += x0; y2 += y0;
	x3 += x0; y3 += y0;
	x4 += x0; y4 += y0;
}


// circle x0y0z0 r0 is in x-y sys
// output ellipse will be on x-y plane in x-y system
void JagGeo::project3DCircleToXYPlane( double x0, double y0, double z0, double r0, double nx0, double ny0,
	                                     double x, double y, double a, double b, double nx )
{
	double nz0 = sqrt( 1.0 - nx0*nx0 - ny0*ny0 );
	nx = nx0;
	a = r0;
	b = r0 * nz0;
	x = x0;
	y = y0;
}


// circle x0y0z0 r0 is in x-y sys
// output ellipse will be on y-z plane in y-z
void JagGeo::project3DCircleToYZPlane( double x0, double y0, double z0, double r0, double nx0, double ny0,
	                                   double y, double z, double a, double b, double ny )
{
	ny = ny0;
	a = r0;
	b = r0 * nx0;
	y = y0;
	z = z0;
}

// circle x0y0z0 r0 is in z-x
// circle x0y0z0 r0 is in x-y sys
// output ellipse will be on y-z plane in y-z
void JagGeo::project3DCircleToZXPlane( double x0, double y0, double z0, double r0, double nx0, double ny0,
	                                   double z, double x, double a, double b, double nz )
{
	nz = sqrt( 1.0 - nx0*nx0 - ny0*ny0 );
	a = r0;
	b = r0 * ny0;
	z = z0;
	x = x0;
}


// rotate only
// nx1, ny1 what value will be in nx2 ny2
void JagGeo::transform3DDirection( double nx1, double ny1, double nx2, double ny2, double &nx, double &ny )
{
	// rotate nx1, ny1, nz1 to nx2 ny2
	double nz1 = sqrt( 1.0 - nx1*nx1 - ny1*ny1 );
	double nz;
	rotate3DCoordGlobal2Local( nx1, ny1, nz1, nx2, ny2, nx, ny, nz );  // nx ny outz are in x'-y'-z'
}

// rotate only
// nx1 what value will be in nx2
void JagGeo::transform2DDirection( double nx1, double nx2, double &nx )
{
	// rotate nx1, ny1, nz1 to nx2 ny2
	double ny1 = sqrt( 1.0 - nx1*nx1 );
	double ny;
	rotate2DCoordGlobal2Local( nx1, ny1, nx2, nx, ny );  // nx ny outz are in x'-y'-z'
	/***
	double ny = sqrt( 1.0 - nx*nx );
	outx = inx*ny - iny*nx;      nx1*ny1 - ny1:nx1 = 0
	outy = inx*nx + iny*ny;      nx1*nx1 + ny1*ny1 = 1
	if nx2 == nx1, nx should be zero n is 1
	***/
}

void JagGeo::samplesOn2DCircle( double x0, double y0, double r, int num, JagVector<JagPoint2D> &samples )
{
	double delta = 2*JAG_PI/(double)num;
	double alpha = 0.0, x, y;
	for ( int i=0; i < num; ++i ) {
		x = r*cos(alpha) + x0;
		y = r*sin(alpha) + y0;
		samples.append(JagPoint2D(x,y) );
		alpha += delta;
	}
}

void JagGeo::samplesOn3DCircle( double x0, double y0, double z0, double r, double nx, double ny, int num, JagVector<JagPoint3D> &samples )
{
	double delta = 2*JAG_PI/(double)num;
	double alpha = 0.0;
	double x,y,z, gx, gy, gz;
	for ( int i=0; i < num; ++i ) {
		x = r*cos(alpha);
		y = r*sin(alpha);
		z = 0.0;
		transform3DCoordLocal2Global( x0, y0, z0, x, y, z, nx, ny, gx, gy, gz );
		samples.append(JagPoint3D( gx, gy, gz) );
		alpha += delta;
	}
}

void JagGeo::samplesOn2DEllipse( double x0, double y0, double a, double b, double nx, int num, JagVector<JagPoint2D> &samples )
{
	double delta = 2*JAG_PI/(double)num;
	double t = 0.0;
	double x,y, gx, gy;
	for ( int i=0; i < num; ++i ) {
		x = a*cos(t);
		y = b*sin(t);
		transform2DCoordLocal2Global( x0, y0, x, y, nx, gx, gy ); 
		samples.append(JagPoint2D( gx, gy) );
		t += delta;
	}
}

void JagGeo::samplesOnEllipsoid( double x0, double y0, double z0, double a, double b, double c, 
								 double nx, double ny, int num, JagVector<JagPoint3D> &samples )
{
	/***
		x = a cos(u)* sin(v)
		y = a sin(u)* sin(v)
		z = cos(v)
		u: 0-2PI     v: 0-PI
	***/
	if ( num < 1 ) return;
	double deltau = 2*JAG_PI/(double)num;
	double deltav = JAG_PI/(double)num;
	double u = 0.0;
	double v = 0.0;
	double x,y, z, gx, gy, gz;
	for ( int i=0; i < num; ++i ) {
		for ( int j=0; j < num; ++j ) {
			x = a*cos(u) * sin(v);
			y = b*sin(u) * sin(v);
			z = b*cos(v);
			transform3DCoordLocal2Global( x0, y0, z0, x, y, z, nx, ny, gx, gy, gz ); 
			samples.append(JagPoint3D( gx, gy, gz) );
			v += deltav;
		}
		u += deltau;
	}
}

// from tip to bottom circle
void JagGeo::sampleLinesOnCone( double x0, double y0, double z0, double r, double h, 
								double nx, double ny, int num, JagVector<JagLine3D> &samples )
{
	double delta = 2*JAG_PI/(double)num;
	double alpha = 0.0;
	double x,y,z, gx, gy, gz;
	JagLine3D line;
	// tip
	line.x1 = 0.0; line.y1 = 0.0; line.z1 = h;
	transform3DCoordLocal2Global( x0,y0,z0, line.x1, line.y1, line.z1, nx, ny, gx, gy, gz );
	line.x1 = gx; line.y1 = gy; line.z1 = gz;
	for ( int i=0; i < num; ++i ) {
		x = r*cos(alpha);
		y = r*sin(alpha);
		z = -h;
		transform3DCoordLocal2Global( x0, y0, z0, x, y, z, nx, ny, gx, gy, gz );
		line.x2 = gx; line.y2 = gy; line.z2 = gz;
		samples.append( line );
		alpha += delta;
	}
}

// from tip to bottom circle
void JagGeo::sampleLinesOnCylinder( double x0, double y0, double z0, double r, double h, 
								    double nx, double ny, int num, JagVector<JagLine3D> &samples )
{
	double delta = 2*JAG_PI/(double)num;
	double alpha = 0.0;
	double x,y,z, gx, gy, gz;
	JagLine3D line;
	// tip
	for ( int i=0; i < num; ++i ) {
		x = r*cos(alpha);
		y = r*sin(alpha);
		z = h;
		transform3DCoordLocal2Global( x0, y0, z0, x, y, z, nx, ny, gx, gy, gz );
		line.x1 = gx; line.y1 = gy; line.z1 = gz;

		z = -h;
		transform3DCoordLocal2Global( x0, y0, z0, x, y, z, nx, ny, gx, gy, gz );
		line.x2 = gx; line.y2 = gy; line.z2 = gz;
		samples.append( line );
		alpha += delta;
	}
}


AbaxDataString JagGeo::convertType2Short( const AbaxDataString &geotypeLong )
{
	const char *p = geotypeLong.c_str();
    if ( 0==strcasecmp(p, "point" ) ) {
		return JAG_C_COL_TYPE_POINT;
	} else if ( 0==strcasecmp(p, "point3d" ) ) {
		return JAG_C_COL_TYPE_POINT3D;
	} else if ( 0==strcasecmp(p, "line" ) ) {
		return JAG_C_COL_TYPE_LINE;
	} else if ( 0==strcasecmp(p, "line3d" ) ) {
		return JAG_C_COL_TYPE_LINE3D;
	} else if ( 0==strcasecmp(p, "circle" ) ) {
		return JAG_C_COL_TYPE_CIRCLE;
	} else if ( 0==strcasecmp(p, "circle3d" ) ) {
		return JAG_C_COL_TYPE_CIRCLE3D;
	} else if ( 0==strcasecmp(p, "sphere" ) ) {
		return JAG_C_COL_TYPE_SPHERE;
	} else if ( 0==strcasecmp(p, "square" ) ) {
		return JAG_C_COL_TYPE_SQUARE;
	} else if ( 0==strcasecmp(p, "square3d" ) ) {
		return JAG_C_COL_TYPE_SQUARE3D;
	} else if ( 0==strcasecmp(p, "cube" ) ) {
		return JAG_C_COL_TYPE_CUBE;
	} else if ( 0==strcasecmp(p, "rectangle" ) ) {
		return JAG_C_COL_TYPE_RECTANGLE;
	} else if ( 0==strcasecmp(p, "rectangle3d" ) ) {
		return JAG_C_COL_TYPE_RECTANGLE3D;
	} else if ( 0==strcasecmp(p, "box" ) ) {
		return JAG_C_COL_TYPE_BOX;
	} else if ( 0==strcasecmp(p, "cone" ) ) {
		return JAG_C_COL_TYPE_CONE;
	} else if ( 0==strcasecmp(p, "triangle" ) ) {
		return JAG_C_COL_TYPE_TRIANGLE;
	} else if ( 0==strcasecmp(p, "triangle3d" ) ) {
		return JAG_C_COL_TYPE_TRIANGLE3D;
	} else if ( 0==strcasecmp(p, "cylinder" ) ) {
		return JAG_C_COL_TYPE_CYLINDER;
	} else if ( 0==strcasecmp(p, "ellipse" ) ) {
		return JAG_C_COL_TYPE_ELLIPSE;
	} else if ( 0==strcasecmp(p, "ellipse3d" ) ) {
		return JAG_C_COL_TYPE_ELLIPSE3D;
	} else if ( 0==strcasecmp(p, "ellipsoid" ) ) {
		return JAG_C_COL_TYPE_ELLIPSOID;
	} else if ( 0==strcasecmp(p, "polygon" ) ) {
		return JAG_C_COL_TYPE_POLYGON;
	} else if ( 0==strcasecmp(p, "polygon3d" ) ) {
		return JAG_C_COL_TYPE_POLYGON3D;
	} else if ( 0==strcasecmp(p, "linestring" ) ) {
		return JAG_C_COL_TYPE_LINESTRING;
	} else if ( 0==strcasecmp(p, "linestring3d" ) ) {
		return JAG_C_COL_TYPE_LINESTRING3D;
	} else if ( 0==strcasecmp(p, "multipoint" ) ) {
		return JAG_C_COL_TYPE_MULTIPOINT;
	} else if ( 0==strcasecmp(p, "multipoint3d" ) ) {
		return JAG_C_COL_TYPE_MULTIPOINT3D;
	} else if ( 0==strcasecmp(p, "multilinestring" ) ) {
		return JAG_C_COL_TYPE_MULTILINESTRING;
	} else if ( 0==strcasecmp(p, "multilinestring3d" ) ) {
		return JAG_C_COL_TYPE_MULTILINESTRING3D;
	} else if ( 0==strcasecmp(p, "multipolygon" ) ) {
		return JAG_C_COL_TYPE_MULTIPOLYGON;
	} else if ( 0==strcasecmp(p, "multipolygon3d" ) ) {
		return JAG_C_COL_TYPE_MULTIPOLYGON3D;
	} else if ( 0==strcasecmp(p, "range" ) ) {
		return JAG_C_COL_TYPE_RANGE;
	} else {
		return "UNKNOWN";
	}
}

// sp was shifted
double JagGeo::safeget( const JagStrSplit &sp, int arg )
{
	double res = 0.0;
	if ( sp.slength() >= arg+1 ) {
		res = jagatof( sp[arg].c_str() );
	}
	return res;
}

// sp was shifted
AbaxDataString JagGeo::safeGetStr( const JagStrSplit &sp, int arg )
{
	AbaxDataString res;
	if ( sp.slength() >= arg+1 ) {
		res = sp[arg];
	}
	return res;
}


bool JagGeo::validDirection( double nx )
{
	if ( nx > 1.0 ) return false;
	if ( nx < -1.0 ) return false;
	return true;
}

bool JagGeo::validDirection( double nx, double ny )
{
	if ( nx > 1.0 || ny > 1.0 ) return false;
	if ( nx < -1.0 || ny < -1.0 ) return false;
	if ( nx*nx + ny*ny > 1.0 ) return false;
	return true;
}



double JagGeo::doSign( const JagPoint2D &p1, const JagPoint2D &p2, const JagPoint2D &p3 )
{
	return (p1.x - p3.x) * ( p2.y - p3.y) - ( p2.x - p3.x) * ( p1.y - p3.y );
}

double JagGeo::distSquarePointToSeg( const JagPoint2D &p, const JagPoint2D &p1, const JagPoint2D &p2 )
{
    double sqlen =  (p2.x - p1.x)*(p2.x - p1.x) + (p2.y - p1.y)*(p2.y - p1.y);
    double dotProduct  = ((p.x - p1.x)*(p2.x - p1.x) + (p.y - p1.y)*(p2.y - p1.y)) / sqlen;
	if ( dotProduct < 0 ) {
  		return (p.x - p1.x)*(p.x - p1.x) + (p.y - p1.y)*(p.y - p1.y);
 	}

 	if ( dotProduct <= 1.0 ) {
  		double p_p1_squareLen = (p1.x - p.x)*(p1.x - p.x) + (p1.y - p.y)*(p1.y - p.y);
  		return ( p_p1_squareLen - dotProduct * dotProduct * sqlen );
 	}

    return (p.x - p2.x)*(p.x - p2.x) + (p.y - p2.y)*(p.y - p2.y);
}

double JagGeo::distance( double fx, double fy, double gx, double gy, int srid )
{
	double res = 0.0;
	if ( 0 == srid ) {
		res = sqrt( (fx-gx)*(fx-gx) + (fy-gy )*(fy-gy) );
	} else if ( JAG_GEO_WGS84 == srid )  {
		const Geodesic& geod = Geodesic::WGS84();
		double s12;
		// (fx fy) = ( lon, lat)
		geod.Inverse(fy, fx, gy, gx, s12 ); 
		res = s12;
	} else {
		res = sqrt( (fx-gx)*(fx-gx) + (fy-gy )*(fy-gy) );
	}

	return res;
}

double JagGeo::distance( double fx, double fy, double fz, 
						 double gx, double gy, double gz,  int srid )
{
	double res = 0.0;
	/***
	if ( 0 == srid ) {
		res = sqrt( (fx-gx)*(fx-gx) + (fy-gy )*(fy-gy) + (fz-gz)*(fz-gz) );
	} else if ( JAG_GEO_WGS84 == srid ) {
		const Geodesic& geod = Geodesic::WGS84();
		double s12;
		// (fx fy, fz) = ( lon, lat, azimuth )
		geod.Inverse(fy, fx, gy, gx, fz, gz, s12 ); 
		res = s12;
	} else {
		res = sqrt( (fx-gx)*(fx-gx) + (fy-gy )*(fy-gy) + (fz-gz)*(fz-gz) );
	}
	***/
	res = sqrt( (fx-gx)*(fx-gx) + (fy-gy )*(fy-gy) + (fz-gz)*(fz-gz) );
	return res;
}

bool JagGeo::jagLE (double f1, double f2 )
{
	if ( f1 < f2 ) return true;
	if ( fabs(f1-f2) < ZERO ) return true;
	return false;
}

bool JagGeo::jagGE (double f1, double f2 )
{
	if ( f1 > f2 ) return true;
	if ( fabs(f1-f2) < ZERO ) return true;
	return false;
}

bool JagGeo::jagEQ (double f1, double f2 )
{
	if ( fabs(f1-f2) < ZERO ) return true;
	return false;
}

// global to local new x'-y'  with just rotation
// coord sys turn clock wise. inx iny was in old sys x-y. outx/out in new coord sys x'-y'
// y' bcomes new unit vector direction
void JagGeo::rotate2DCoordGlobal2Local( double inx, double iny, double nx, double &outx, double &outy )
{
	if ( jagLE(nx, ZERO) || fabs(nx) > 1.0  ) {
		outx = inx;
		outy = iny;
		return;
	}

	double ny = sqrt( 1.0 - nx*nx );
	outx = inx*ny - iny*nx;
	outy = inx*nx + iny*ny;
}

// local to global
// coord sys turn counter-clock wise. inx iny was in x'-y'. outx/out in old coord  x-y.
// nx is unit vector inx iny is in unit vector-as-y axis plane,
// inx iny are relative locally on unitvector plane where unit vector is y-axis
// outx outy will be in x-y where unit vector is measured
void JagGeo::rotate2DCoordLocal2Global( double inx, double iny, double nx, double &outx, double &outy )
{
	if ( jagLE(nx, ZERO) || fabs(nx) > 1.0 ) {
		outx = inx;
		outy = iny;
		return;
	}

	double ny = sqrt( 1.0 - nx*nx );
	outx = inx*ny + iny*nx;
	outy = iny*ny - inx*nx;
}



// new z-aix is along unit vector
// local is new z-axis
void JagGeo::rotate3DCoordGlobal2Local( double inx, double iny, double inz, double nx, double ny,
                                    double &outx, double &outy, double &outz )
{
	double n2 = nx*nx + ny*ny;
	if ( jagIsPosZero(n2) || n2 > 1.0 ) {
		outx = inx;
		outy = iny;
		outz = inz;
		return;
	}

	double sqr = sqrt( n2 );
	double nz = sqrt(1.0 - n2);
	outx = (inx*ny - iny*nx)/sqr;
	outy = (inx*nx*nz + iny*ny*nz)/sqr - inz*sqr;
	outz = inx*nx + iny*ny + inz*nz;
}

// local to global
// given loccal coords on vector-as-z axis, find original real-world coords
// rotate around origin
void JagGeo::rotate3DCoordLocal2Global( double inx, double iny, double inz, double nx, double ny,
                                    double &outx, double &outy, double &outz )
{
	double n2 = nx*nx + ny*ny;
	if ( jagIsPosZero(n2) || n2 > 1.0 ) {
		outx = inx;
		outy = iny;
		outz = inz;
		return;
	}

	double sqr = sqrt( n2 );
	double nz = sqrt(1.0 - n2);
	outx = (inx*ny + iny*nx*nz)/sqr + inz*nz;
	outy = (-inx*nx + iny*ny*nz)/sqr + inz*ny;
	outz =  -iny*sqr + inz*nz;
}

// global to local
// inx0/iny0:  new coord origin
// inx iny are on nx plane locally, inx0 iny are its origin of nx plane wrf to real x-y system
void JagGeo::transform2DCoordGlobal2Local( double outx0, double outy0, double inx, double iny, double nx, 
							   double &outx, double &outy )
{
	inx -= outx0;
	iny -= outy0;
	rotate2DCoordGlobal2Local( inx, iny, nx, outx, outy );  // outx outy are in real world but wrf to nx plane
	// outx += inx0; // to real x-y sys
	// outy += iny0; // to real x-y sys
}

// coord sys turn counter-clock wise. inx iny was in x'-y'. outx/out in old coord  x-y.
// nx is unit vector inx iny is in unit vector-as-y axis plane,
// inx iny are relative locally on unitvector plane where unit vector is y-axis
// outx outy will be in x-y where unit vector is measured
// x0 y0 is coord of shape in x-y sys
// inx iny are in coord of x'-y' local system
// outx/outy are in xy-sys absolute coords
// local to global transform
void JagGeo::transform2DCoordLocal2Global( double x0, double y0, double inx, double iny, double nx, double &outx, double &outy )
{
	// void JagGeo::rotate2DCoordLocal2Global( double inx, double iny, double nx, double &outx, double &outy )
	rotate2DCoordLocal2Global( inx, iny, nx, outx, outy );
	outx += x0;
	outy += y0;
	/***
	prt(("s0738 transform2DCoordLocal2Global inx=%.4f iny=%.4f x0=%f y0=%f nx=%f outx=%f outy=%f\n",
			inx, iny, x0, y0, nx, outx, outy ));
	***/
}

// global to local
// inx iny are on nx plane locally, inx0 iny are its origin of nx plane wrf to real x-y system
void JagGeo::transform3DCoordGlobal2Local( double outx0, double outy0, double outz0, double inx, double iny, double inz, 
							   double nx,  double ny,
							   double &outx, double &outy, double &outz )
{
	inx -= outx0;
	iny -= outy0;
	inz -= outz0;
	rotate3DCoordGlobal2Local( inx, iny, inz, nx, ny, outx, outy, outz );  // outx outy outz are in x'-y'-z'
}

// global to local
// inx iny are on nx plane locally, inx0 iny are its origin of nx plane wrf to real x-y system
void JagGeo::transform3DCoordLocal2Global( double inx0, double iny0, double inz0, double inx, double iny, double inz, 
							   double nx,  double ny,
							   double &outx, double &outy, double &outz )
{
	rotate3DCoordLocal2Global( inx, iny, inz, nx, ny, outx, outy, outz );  // outx outy outz are in x'-y'-z'
	outx += inx0; // to real x-y sys
	outy += iny0; // to real x-y sys
	outz += inz0; // to real x-y sys
}

double JagGeo::jagMin( double x1, double x2, double x3 )
{
	double m = jagmin(x1, x2);
	return  jagmin(m, x3);
}
double JagGeo::jagMax( double x1, double x2, double x3 )
{
	double m = jagmax(x1, x2);
	return  jagmax(m, x3);
}


bool JagGeo::bound3DDisjoint( double x1, double y1, double z1, double w1, double d1, double h1,
                double x2, double y2, double z2, double w2, double d2, double h2 )
{
	double mxd1 = jagMax(w1,d1,h1);
	double mxd2 = jagMax(w2,d2,h2);
	if (  x1+mxd1 < x2-mxd2 || x1-mxd1 > x2+mxd2 ) return true;
	if (  y1+mxd1 < y2-mxd2 || y1-mxd1 > y2+mxd2 ) return true;
	if (  z1+mxd1 < z2-mxd2 || z1-mxd1 > z2+mxd2 ) return true;
	return false;
}

bool JagGeo::bound2DDisjoint( double x1, double y1, double w1, double h1, double x2, double y2, double w2, double h2 )
{
	double mxd1 = jagmax(w1,h1);
	double mxd2 = jagmax(w2,h2);
	if (  x1+mxd1 < x2-mxd2 || x1-mxd1 > x2+mxd2 ) return true;
	if (  y1+mxd1 < y2-mxd2 || y1-mxd1 > y2+mxd2 ) return true;
	return false;
}



// point px py  line: x1 y1 x2 y2
double JagGeo::squaredDistancePoint2Line( double px, double py, double x1, double y1, double x2, double y2 )
{
	if ( jagEQ(x1, x2) && jagEQ(y1, y2)  ) {
		return (px-x1)*(px-x1) + (py-y1)*(py-y1);
	}

	if ( jagEQ(x1, x2) ) {
		return (px-x1)*(px-x1);
	}

	double f1 = (y2-y1)*px - (x2-x1)*py + x2*y1 - y2*x1;
	double f2 = (y2-y1)*(y2-y1) + (x2-x1)*(x2-x1);
	return f1*f1/f2;
}

// return 0: disjoint, no touching points
//        1: tagent with one touching point
//        2: insersect by two touching points
// Normal ellipse
// Can be applied to circle: a=b=r
int JagGeo::relationLineNormalEllipse( double x1, double y1, double x2, double y2, double a, double b )
{
	if ( jagmax(x1,x2) < -a ) return 0;
	if ( jagmax(y1,y2) < -b ) return 0;
	if ( jagmin(x1,x2) > a ) return 0;
	if ( jagmin(y1,y2) > b ) return 0;
	if ( jagEQ(a, 0.0) ) return 0;
	if ( jagEQ(b, 0.0) ) return 0;

	// line y = mx + c
	if ( jagEQ(x1,x2) ) {
		// vertical line
		if ( jagEQ(x1, -a) || jagEQ(x1, a) )  {
			// look at  y1 y2
			if ( jagmin(y1,y2) > 0 ) return 0;
			if ( jagmax(y1,y2) < 0 ) return 0;
			return 1;  // tangent
		} else if ( x1 < -a || x1 > a ) {
			return 0;  // disjoint
		} else {
			// b2 * x2 + a2 * y2 = a2 *b2
			// y2 = ( a2*b2 - b2*x2 ) /a2;
			// y1 = sqrt( a2*b2 - b2*x1^2 ) /a;
			// y2 = -y1;
			double sy1 = b*sqrt( 1.0 - x1*x1/(a*a) );
			double sy2 = -sy1;
			if ( jagmin(y1,y2) > jagmax(sy1,sy2) ) return 0;
			if ( jagmax(y1,y2) < jagmin(sy1,sy2) ) return 0;
			return 2;
		}
	}

	double m = (y2-y1)/(x2-x1);
	double c = y1 - m*x1;
	double D = a*a*m*m + b*b - c*c;
	if ( D < 0.0 ) return 0;
	double sqrtD = sqrt(D);

	if  ( jagEQ(D, 0.0) ) {
		double denom = c*c;
		double sx = -a*a*m*c/denom;
		double sy = b*b*c/denom;
		if ( jagmin(x1,x2) > sx ) return 0;
		if ( jagmax(x1,x2) < sx ) return 0;
		if ( jagmin(y1,y2) > sy ) return 0;
		if ( jagmax(y1,y2) < sy ) return 0;
		return 1;
	} 

	double denom = D + c*c;
	double sx1 = a*( -a*m*c + b*sqrtD )/denom;
	double sy1 = b*( b*c + a*m*sqrtD )/denom;

	double sx2 = a*( -a*m*c - b*sqrtD )/denom;
	double sy2 = b*( b*c - a*m*sqrtD )/denom;

	if ( jagmin(x1,x2) > jagmax(sx1,sx2) ) return 0;
	if ( jagmax(x1,x2) < jagmin(sx1,sx2) ) return 0;

	if ( jagmin(y1,y2) > jagmax(sy1,sy2) ) return 0;
	if ( jagmax(y1,y2) < jagmin(sy1,sy2) ) return 0;

	return 2;

}
	
// return 0: disjoint, no touching points
//        1: tagent with one touching point
//        2: insersect by two touching points
// rotated ellipse
// Can be applied to circle: a=b=r
int JagGeo::relationLineCircle( double x1, double y1, double x2, double y2,
								 double x0, double y0, double r )
{
    double px1, py1, px2, py2;
	px1 = x1 - x0;
	py1 = y1 - y0;
	px2 = x2 - x0;
	py2 = y2 - y0;
	return relationLineNormalEllipse(px1, py1, px2, py2, r, r );
}

// return 0: disjoint, no touching points
//        1: tagent with one touching point
//        2: insersect by two touching points
// rotated ellipse
// Can be applied to circle: a=b=r
int JagGeo::relationLineEllipse( double x1, double y1, double x2, double y2,
								 double x0, double y0, double a, double b, double nx )
{
    double px1, py1, px2, py2;
	transform2DCoordGlobal2Local( x0, y0, x1, y1, nx, px1, py1 );
	transform2DCoordGlobal2Local( x0, y0, x2, y2, nx, px2, py2 );
	return relationLineNormalEllipse(px1, py1, px2, py2, a, b );
}


// 3D point to line distance
// point px py pz  line: x1 y1 z1 x2 y2 z2
double JagGeo::squaredDistance3DPoint2Line( double px, double py, double pz, 
							double x1, double y1, double z1, double x2, double y2, double z2 )
{
	if ( jagEQ(x1, x2) && jagEQ(y1, y2) && jagEQ(z1, z2)  ) {
		return (px-x1)*(px-x1) + (py-y1)*(py-y1) + (pz-z1)*(pz-z1);
	}

	if ( jagEQ(x1, x2) && jagEQ(y1, y2) ) {
		return (px-x1)*(px-x1) + (py-y1)*(py-y1);
	}

	double a = x2-x1;
	double b= y2-y1;
	double c = z2-z1;
	double f1 = (y1-y2)*c - b*(z1-z2);
	double f2 = (z1-z2)*a - c*(x1-x2);
	double f3 = (x1-x2)*b - a*(y1-y2);
	return (f1*f1 + f2*f2 + f3*f3)/(a*a+b*b+c*c);
}

bool JagGeo::line3DIntersectLine3D( double x1, double y1, double z1, double x2, double y2, double z2,
							double px1, double py1, double pz1, double px2, double py2, double pz2 )
{
	//prt(("s6703 line3DIntersectLine3D x1=%.1f y1=%.1f z1=%.1f  x2=%.1f y2=%.1f z2=%.1f\n", x1,y1,z1,x2,y2,z2 ));
	//prt(("s6703 line3DIntersectLine3D px1=%.1f py1=%.1f pz1=%.1f  px2=%.1f py2=%.1f pz2=%.1f\n", px1,py1,pz1,px2,py2,pz2 ));
	double a1 = x2-x1;
	double b1= y2-y1;
	double c1 = z2-z1;

	double a2 = px2-px1;
	double b2= py2-py1;
	double c2 = pz2-pz1;

	double D = (px1-x1)*b1*c2 + (py1-y1)*c1*a2 + (pz1-z1)*b2*a1
	           - (px1-x1)*b2*c1 - (py1-y1)*a1*c2 - (pz1-z1)*b1*a2;

    prt(("s6704 D = %f\n", D ));

	if ( fabs(D) < 0.001f ) {
		prt(("s7681 D is zero, return true\n" ));
		return true;
	}
	prt(("s7681 D is not zero, return true\n" ));
	return false;

}


// 1: tangent
// 2: intersect with two two touching points
// 0: disjoint
int JagGeo::relationLine3DSphere( double x1, double y1, double z1, double x2, double y2, double z2,
									double x3, double y3, double z3, double r )
{
	double a = (x2-x1)*(x2-x1) + (y2-y1)*(y2-y1) + (z2-z1)*(z2-z1);
	double b = 2*( (x2-x1)*(x1-x3) + (y2-y1)*(y1-y3) + (z2-z1)*(z1-z3) );
	double c = x3*x3 + y3*y3 + z3*z3 + x1*x1 + y1*y1 + z1*z1 - 2*(x3*x1+y3*y1+z3*z1) - r*r;

	double D = b*b - 4.0*a*c;
	if ( jagEQ(D, 0.0) ) {
		return 1;
	} else if ( D < 0.0 ) {
		return 0;
	} else {
		return 2;
	}

}

bool JagGeo::point3DWithinRectangle2D( double px, double py, double pz,
							double x0, double y0, double z0,
							double a, double b, double nx, double ny, bool strict )
{
	double locx, locy, locz;
	transform3DCoordGlobal2Local( x0, y0, z0, px, py, pz, nx, ny, locx, locy, locz );
	if ( locIn2DCenterBox( px,py,a,b, strict ) ) { return true; }
	return false;
}

void JagGeo::cornersOfRectangle( double w, double h, JagPoint2D p[] )
{
	p[0].x = -w; p[0].y = -h;
	p[1].x = -w; p[1].y = h;
	p[2].x = w; p[2].y = h;
	p[3].x = w; p[3].y =- h;
}

void JagGeo::cornersOfRectangle3D( double w, double h, JagPoint3D p[] )
{
	p[0].x = -w; p[0].y = -h; p[0].z = 0.0;
	p[1].x = -w; p[1].y = h; p[1].z = 0.0;
	p[2].x = w; p[2].y = h; p[2].z = 0.0;
	p[3].x = w; p[3].y =- h; p[3].z = 0.0;
}

void JagGeo::cornersOfBox( double w, double d, double h, JagPoint3D p[] )
{
	p[0].x = -w; p[0].y = -d; p[0].z = -h;
	p[1].x = -w; p[1].y = d; p[1].z = -h;
	p[2].x = w; p[2].y = d; p[2].z = -h;
	p[3].x = w; p[3].y = -d; p[3].z = -h;

	p[4].x = -w; p[4].y = -d; p[4].z = h;
	p[5].x = -w; p[5].y = d; p[5].z = h;
	p[6].x = w; p[6].y = d; p[6].z = h;
	p[7].x = w; p[7].y = -d; p[7].z = h;
}

void JagGeo::edgesOfRectangle( double w, double h, JagLine2D line[] )
{
	JagPoint2D p[4];
	cornersOfRectangle(w,h, p);

	line[0].x1 = p[0].x; 
	line[0].y1 = p[0].y; 
	line[0].x2 = p[1].x; 
	line[0].y2 = p[1].y; 

	line[1].x1 = p[1].x; 
	line[1].y1 = p[1].y; 
	line[1].x2 = p[2].x; 
	line[1].y2 = p[2].y; 

	line[2].x1 = p[2].x; 
	line[2].y1 = p[2].y; 
	line[2].x2 = p[3].x; 
	line[2].y2 = p[3].y; 

	line[3].x1 = p[3].x; 
	line[3].y1 = p[3].y; 
	line[3].x2 = p[1].x; 
	line[3].y2 = p[1].y; 
}

void JagGeo::edgesOfRectangle3D( double w, double h, JagLine3D line[] )
{
	JagLine2D line2d[4];
	edgesOfRectangle( w,h, line2d);
	for ( int i=0; i<4; ++i )
	{
		line[i].x1 = line2d[i].x1;
		line[i].y1 = line2d[i].y1;
		line[i].x2 = line2d[i].x2;
		line[i].y2 = line2d[i].y2;
	}
}


// line: 12 edges
void JagGeo::edgesOfBox( double w, double d, double h, JagLine3D line[] )
{
	JagPoint3D p[8];
	cornersOfBox( w,d,h, p );

	line[0].x1 = p[0].x; 
	line[0].y1 = p[0].y; 
	line[0].z1 = p[0].z; 
	line[0].x2 = p[1].x; 
	line[0].y2 = p[1].y; 
	line[0].z2 = p[1].z; 


	line[1].x1 = p[1].x; 
	line[1].y1 = p[1].y; 
	line[1].z1 = p[1].z; 
	line[1].x2 = p[2].x; 
	line[1].y2 = p[2].y; 
	line[1].z2 = p[2].z; 

	line[2].x1 = p[2].x; 
	line[2].y1 = p[2].y; 
	line[2].z1 = p[2].z; 
	line[2].x2 = p[3].x; 
	line[2].y2 = p[3].y; 
	line[2].z2 = p[3].z; 

	line[3].x1 = p[3].x; 
	line[3].y1 = p[3].y; 
	line[3].z1 = p[3].z; 
	line[3].x2 = p[0].x; 
	line[3].y2 = p[0].y; 
	line[3].z2 = p[0].z; 

	line[4].x1 = p[4].x; 
	line[4].y1 = p[4].y; 
	line[4].z1 = p[4].z; 
	line[4].x2 = p[5].x; 
	line[4].y2 = p[5].y; 
	line[4].z2 = p[5].z; 

	line[5].x1 = p[5].x; 
	line[5].y1 = p[5].y; 
	line[5].z1 = p[5].z; 
	line[5].x2 = p[6].x; 
	line[5].y2 = p[6].y; 
	line[5].z2 = p[6].z; 

	line[6].x1 = p[6].x; 
	line[6].y1 = p[6].y; 
	line[6].z1 = p[6].z; 
	line[6].x2 = p[7].x; 
	line[6].y2 = p[7].y; 
	line[6].z2 = p[7].z; 

	line[7].x1 = p[7].x; 
	line[7].y1 = p[7].y; 
	line[7].z1 = p[7].z; 
	line[7].x2 = p[4].x; 
	line[7].y2 = p[4].y; 
	line[7].z2 = p[4].z; 


	// vertical ones
	line[8].x1 = p[0].x; 
	line[8].y1 = p[0].y; 
	line[8].z1 = p[0].z; 
	line[8].x2 = p[4].x; 
	line[8].y2 = p[4].y; 
	line[8].z2 = p[4].z; 

	line[9].x1 = p[1].x; 
	line[9].y1 = p[1].y; 
	line[9].z1 = p[1].z; 
	line[9].x2 = p[5].x; 
	line[9].y2 = p[5].y; 
	line[9].z2 = p[5].z; 

	line[10].x1 = p[2].x; 
	line[10].y1 = p[2].y; 
	line[10].z1 = p[2].z; 
	line[10].x2 = p[6].x; 
	line[10].y2 = p[6].y; 
	line[10].z2 = p[6].z; 

	line[11].x1 = p[3].x; 
	line[11].y1 = p[3].y; 
	line[11].z1 = p[3].z; 
	line[11].x2 = p[7].x; 
	line[11].y2 = p[7].y; 
	line[11].z2 = p[7].z; 

}

void JagGeo::transform3DLinesLocal2Global( double x0, double y0, double z0, double nx0, double ny0, int num, JagLine3D line[] )
{
	JagLine3D L;
	double gx, gy, gz;
	for ( int i=0; i<num; ++i ) {
		transform3DCoordLocal2Global( x0,y0,z0, line[i].x1, line[i].y1, line[i].z1, nx0, ny0, gx, gy, gz );
		L.x1 = gx; L.y1 = gy; L.z1 = gz;
		transform3DCoordLocal2Global( x0,y0,z0, line[i].x2, line[i].y2, line[i].z2, nx0, ny0, gx, gy, gz );
		L.x2 = gx; L.y2 = gy; L.z2 = gz;
		line[i] = L;
	}
}

void JagGeo::transform3DLinesGlobal2Local( double x0, double y0, double z0, double nx0, double ny0, int num, JagLine3D line[] )
{
	JagLine3D L;
	double gx, gy, gz;
	for ( int i=0; i<num; ++i ) {
		transform3DCoordGlobal2Local( x0,y0,z0, line[i].x1, line[i].y1, line[i].z1, nx0, ny0, gx, gy, gz );
		L.x1 = gx; L.y1 = gy; L.z1 = gz;
		transform3DCoordLocal2Global( x0,y0,z0, line[i].x2, line[i].y2, line[i].z2, nx0, ny0, gx, gy, gz );
		L.x2 = gx; L.y2 = gy; L.z2 = gz;
		line[i] = L;
	}
}


void JagGeo::transform2DLinesLocal2Global( double x0, double y0, double nx0, int num, JagLine2D line[] )
{
	JagLine2D L;
	double gx, gy;
	for ( int i=0; i<num; ++i ) {
		transform2DCoordLocal2Global( x0,y0,line[i].x1, line[i].y1, nx0, gx, gy );
		L.x1 = gx; L.y1 = gy; 
		transform2DCoordLocal2Global( x0,y0,line[i].x2, line[i].y2, nx0, gx, gy );
		L.x2 = gx; L.y2 = gy;
		line[i] = L;
	}
}

void JagGeo::transform2DLinesGlobal2Local( double x0, double y0, double nx0, int num, JagLine2D line[] )
{
	JagLine2D L;
	double gx, gy;
	for ( int i=0; i<num; ++i ) {
		transform2DCoordGlobal2Local( x0,y0,line[i].x1, line[i].y1, nx0, gx, gy );
		L.x1 = gx; L.y1 = gy; 
		transform2DCoordGlobal2Local( x0,y0,line[i].x2, line[i].y2, nx0, gx, gy );
		L.x2 = gx; L.y2 = gy;
		line[i] = L;
	}
}

bool JagGeo::line3DIntersectNormalRectangle( const JagLine3D &line, double w, double h )
{
	if ( jagEQ(line.z1, 0.0) &&  jagEQ(line.z2, 0.0) ) {
		if ( jagGE(line.x1, -w) && jagLE(line.x1, w) && jagGE(line.y1, -h) && jagLE(line.y1, h) ) {
			return true;
		}
		if ( jagGE(line.x2, -w) && jagLE(line.x2, w) && jagGE(line.y2, -h) && jagLE(line.y2, h) ) {
			return true;
		}
	}

	if ( jagEQ(line.z1, line.z2) ) {
		return false;
	}

	double x0 = line.x1 + (line.x2-line.x1)*line.z1/(line.z2-line.z1);
	double y0 = line.y1 + (line.y2-line.y1)*line.z1/(line.z2-line.z1);

	if ( jagGE(x0, -w) && jagLE(x0, w) && jagGE(y0, -h) && jagLE( y0, h) ) {
		return true;
	}

	return false;
}


bool JagGeo::line3DIntersectRectangle3D( const JagLine3D &line, const JagRectangle3D &rect )
{
	return line3DIntersectRectangle3D(line, rect.x, rect.y, rect.z, rect.w, rect.h, rect.nx, rect.ny );
}

// line has global coord
bool JagGeo::line3DIntersectRectangle3D( const JagLine3D &line, double x0, double y0, double z0, double w, double h, double nx, double ny )
{
	double locx, locy, locz;
	JagLine3D L;
	transform3DCoordGlobal2Local( x0,y0,z0, line.x1, line.y1, line.z1, nx, ny, locx, locy, locz );
	L.x1 = locx;
	L.y1 = locy;
	L.z1 = locz;

	transform3DCoordGlobal2Local( x0,y0,z0, line.x2, line.y2, line.z2, nx, ny, locx, locy, locz );
	L.x2 = locx;
	L.y2 = locy;
	L.z2 = locz;

	return line3DIntersectNormalRectangle(L, w, h ); 
}



bool JagGeo::line2DIntersectNormalRectangle( const JagLine2D &line, double w, double h )
{
	if ( jagGE(line.x1, -w) && jagLE(line.x1, w) && jagGE(line.y1, -h) && jagLE(line.y1, h) ) {
		return true;
	}
	if ( jagGE(line.x2, -w) && jagLE(line.x2, w) && jagGE(line.y2, -h) && jagLE(line.y2, h) ) {
		return true;
	}

	return false;
}

// line has global coord
bool JagGeo::line2DIntersectRectangle( const JagLine2D &line, double x0, double y0, double w, double h, double nx )
{
	double locx, locy;
	JagLine2D L;
	transform2DCoordGlobal2Local( x0,y0, line.x1, line.y1, nx, locx, locy );
	L.x1 = locx;
	L.y1 = locy;

	transform2DCoordGlobal2Local( x0,y0, line.x2, line.y2, nx, locx, locy );
	L.x2 = locx;
	L.y2 = locy;

	return line2DIntersectNormalRectangle(L, w, h ); 
}


bool JagGeo::line3DIntersectNormalEllipse( const JagLine3D &line, double w, double h )
{
	if ( jagEQ(line.z1, 0.0) &&  jagEQ(line.z2, 0.0) ) {
		if ( pointWithinNormalEllipse( line.x1, line.y1, w,h, false ) ) return true;
		if ( pointWithinNormalEllipse( line.x2, line.y2, w,h, false ) ) return true;
	}

	if ( jagEQ(line.z1, line.z2) ) {
		return false;
	}

	double x0 = line.x1 + (line.x2-line.x1)*line.z1/(line.z2-line.z1);
	double y0 = line.y1 + (line.y2-line.y1)*line.z1/(line.z2-line.z1);
	if ( pointWithinNormalEllipse( x0, y0, w,h, false ) ) return true;
	return false;
}

// line has global coord
bool JagGeo::line3DIntersectEllipse3D( const JagLine3D &line, double x0, double y0, double z0, double w, double h, double nx, double ny )
{
	double locx, locy, locz;
	JagLine3D L;
	transform3DCoordGlobal2Local( x0,y0,z0, line.x1, line.y1, line.z1, nx, ny, locx, locy, locz );
	L.x1 = locx;
	L.y1 = locy;
	L.z1 = locz;

	transform3DCoordGlobal2Local( x0,y0,z0, line.x2, line.y2, line.z2, nx, ny, locx, locy, locz );
	L.x2 = locx;
	L.y2 = locy;
	L.z2 = locz;

	return line3DIntersectNormalEllipse(L, w, h ); 
}

bool JagGeo::lineIntersectLine( double x1, double y1, double x2, double y2,
								double x3, double y3, double x4, double y4  )
{
	#if 0
	prt(("s7702 lineIntersectLine x1=%f y1=%f x2=%f y2=%f  -- x3=%f y3=%f x4=%f y4=%f\n",
		   x1, y1, x2, y2, x3, y3, x4, y4 ));
   #endif

	if ( jagEQ( x1, x2) && jagEQ( x3, x4) ) {
		return false;
	}

	if ( jagEQ( x1, x2) ) {
		// y = mx+b
		double m = (y3-y4)/(x3-x4);
		double b = y4 - m*x4;
		double y0 = m*x1 + b;
		if ( jagLE(y0, jagmax(y1,y2) ) && jagGE(y0, jagmin(y1,y2) ) ) {
			//prt(("s6802 yes\n" ));
			return true;
		} else {
			return false;
		}
	}

	if ( jagEQ( x3, x4) ) {
		// y = mx+b
		double m = (y1-y2)/(x1-x2);
		double b = y1 - m*x1;
		double y0 = m*x3 + b;
		if ( jagLE(y0, jagmax(y3,y4) ) && jagGE(y0, jagmin(y3,y4) ) ) {
			//prt(("s6803 yes\n" ));
			return true;
		} else {
			return false;
		}
	}

	double A1 = (y1-y2)/(x1-x2);
	double A2 = (y3-y4)/(x3-x4);
	if ( jagEQ( A1, A2 ) ) {
		//prt(("s2091 A1=%f A2=%f false\n", A1,A2 ));
		return false;
	}

	double b1 = y1 - A1*x1;
	double b2 = y3 - A2*x3;
	double Xa = (b2 - b1) / (A1 - A2);

	if ( (Xa < jagmax( jagmin(x1,x2), jagmin(x3,x4) )) || (Xa > jagmin( jagmax(x1,x2), jagmax(x3,x4) )) ) {
		//prt(("s7521 intersection is out of bound false\n" ));
    	return false; // intersection is out of bound
	} else {
		//prt(("s6805 yes Xa=%f b1=%f b2=%f A1=%f A2=%f\n", Xa, b1, b2, A1, A2 ));
    	return true;
	}
}

bool JagGeo::lineIntersectLine(  const JagLine2D &line1, const JagLine2D &line2 )
{
	return lineIntersectLine( line1.x1, line1.y1, line1.x2, line1.y2, line2.x1, line2.y1, line2.x2, line2.y2 );
}

void JagGeo::edgesOfTriangle( double x1, double y1, double x2, double y2, double x3, double y3, JagLine2D line[] )
{
	line[0].x1 = x1;
	line[0].y1 = y1;
	line[0].x2 = x2;
	line[0].y2 = y2;

	line[1].x1 = x2;
	line[1].y1 = y2;
	line[1].x2 = x3;
	line[1].y2 = y3;
	
	line[2].x1 = x3;
	line[2].y1 = y3;
	line[2].x2 = x1;
	line[2].y2 = y1;
}

void JagGeo::edgesOfTriangle3D( double x1, double y1, double z1,
								double x2, double y2, double z2,
								double x3, double y3, double z3,
								JagLine3D line[] )
{
	line[0].x1 = x1;
	line[0].y1 = y1;
	line[0].z1 = z1;
	line[0].x2 = x2;
	line[0].y2 = y2;
	line[0].z2 = z2;

	line[1].x1 = x2;
	line[1].y1 = y2;
	line[1].z1 = z2;
	line[1].x2 = x3;
	line[1].y2 = y3;
	line[1].z2 = z3;
	
	line[2].x1 = x3;
	line[2].y1 = y3;
	line[2].z1 = z3;
	line[2].x2 = x1;
	line[2].y2 = y1;
	line[2].z2 = z1;
}

void JagGeo::pointsOfTriangle3D( double x1, double y1, double z1,
								double x2, double y2, double z2,
								double x3, double y3, double z3, JagPoint3D p[] )
{
	p[0].x = x1;
	p[0].y = y1;
	p[0].z = z1;

	p[1].x = x2;
	p[1].y = y2;
	p[1].z = z2;

	p[2].x = x3;
	p[2].y = y3;
	p[2].z = z3;
}


bool JagGeo::line3DIntersectLine3D( const JagLine3D &line1, const JagLine3D &line2 )
{
	return line3DIntersectLine3D( line1.x1, line1.y1, line1.z1,  line1.x2, line1.y2, line1.z2,
								  line2.x1, line2.y1, line2.z1,  line2.x2, line2.y2, line2.z2 );
}

void JagGeo::transform3DEdgesLocal2Global( double x0, double y0, double z0, double nx0, double ny0, int num, JagPoint3D p[] )
{
	/***
	JagPoint3D P;
	double gx, gy, gz;
	for ( int i=0; i<num; ++i ) {
		transform3DCoordLocal2Global( x0,y0,z0, p[i].x, p[i].y, p[i].z, nx0, ny0, gx, gy, gz );
		P.x = gx; P.y = gy; P.z = gz;
		p[i] = P;
	}
	***/
	for ( int i=0; i<num; ++i ) {
		transform3DCoordLocal2Global( x0,y0,z0, p[i].x, p[i].y, p[i].z, nx0, ny0, p[i].x, p[i].y, p[i].z );
	}
}

void JagGeo::transform2DEdgesLocal2Global( double x0, double y0, double nx0, int num, JagPoint2D p[] )
{
	for ( int i=0; i<num; ++i ) {
		transform2DCoordLocal2Global( x0,y0, p[i].x, p[i].y, nx0, p[i].x, p[i].y );
	}
}

double JagGeo::distanceFromPoint3DToPlane(  double x, double y, double z, 
										   double x0, double y0, double z0, double nx0, double ny0 )
{
	double nz0 = sqrt(1.0- nx0*nx0 - ny0*ny0 );
	double D = (nx0*x0 + ny0*y0 + nz0* z0); 
	return fabs( nx0*x + ny0*y + nz0*z - D);
}

void JagPoint2D::transform( double x0, double y0, double nx0 )
{
	JagGeo::transform2DCoordLocal2Global( x0,y0, x,y, nx0, x, y );
}

void JagPoint3D::transform( double x0, double y0, double z0, double nx0, double ny0 )
{
	JagGeo::transform3DCoordLocal2Global( x0,y0,z0, x,y,z, nx0, ny0, x, y, z );
}

void JagLine2D::transform( double x0, double y0, double nx0 )
{
	JagGeo::transform2DCoordLocal2Global( x0,y0, x1,y1, nx0, x1, y1 );
	JagGeo::transform2DCoordLocal2Global( x0,y0, x2,y2, nx0, x2, y2 );
}

void JagLine3D::transform( double x0, double y0, double z0, double nx0, double ny0  )
{
	JagGeo::transform3DCoordLocal2Global( x0,y0,z0, x1,y1,z1, nx0,ny0, x1, y1, z1 );
	JagGeo::transform3DCoordLocal2Global( x0,y0,z0, x2,y2,z2, nx0,ny0, x2,y2,z2 );
}


void JagRectangle3D::transform( double x0, double y0, double z0, double nx0, double ny0 )
{
	// xyx nx ny is self value
	JagGeo::transform3DCoordLocal2Global( x0,y0,z0, x,y,z, nx0, ny0, x, y, z );
	// transform3DDirection( double nx1, double ny1, double nx2, double ny2, double &nx, double &ny );
	JagGeo::transform3DDirection( nx, ny, nx0, ny0, nx, ny );
}

void JagTriangle3D::transform( double x0, double y0, double z0, double nx0, double ny0 )
{
	JagGeo::transform3DCoordLocal2Global( x0,y0,z0, x1,y1,z1, nx0, ny0, x1, y1, z1 );
	JagGeo::transform3DCoordLocal2Global( x0,y0,z0, x2,y2,z2, nx0, ny0, x2, y2, z2 );
	JagGeo::transform3DCoordLocal2Global( x0,y0,z0, x3,y3,z3, nx0, ny0, x3, y3, z3 );
}

// local 6 six surfaces
void JagGeo::surfacesOfBox(double w, double d, double h, JagRectangle3D rect[] )
{
	// top
	rect[0].x = 0.0; rect[0].y = 0.0; rect[0].z = h;
	rect[0].nx = 0.0; rect[0].ny = 0.0; 
	rect[0].w = w; rect[0].h = d; 

	// right
	rect[1].x = w; rect[1].y = 0.0; rect[1].z = 0;
	rect[1].nx = 1.0; rect[1].ny =0.0; 
	rect[1].w = d; rect[0].h = h; 

	// bottom
	rect[2].x = 0; rect[2].y = 0.0; rect[2].z = -h;
	rect[2].nx = 0.0; rect[2].ny =0.0; 
	rect[2].w = w; rect[2].h = d; 

	// left
	rect[3].x = -w; rect[3].y = 0.0; rect[3].z = 0.0;
	rect[3].nx = 1.0; rect[3].ny =0.0; 
	rect[3].w = d; rect[3].h = h; 

	// front
	rect[4].x = 0.0; rect[4].y = -d; rect[4].z = 0.0;
	rect[4].nx = 0.0; rect[4].ny =1.0; 
	rect[4].w = h; rect[4].h = w; 

	// back
	rect[5].x = 0.0; rect[5].y = d; rect[5].z = 0.0;
	rect[5].nx = 0.0; rect[5].ny =1.0; 
	rect[5].w = h; rect[5].h = w; 
}

// local 6 six surfaces
void JagGeo::triangleSurfacesOfBox(double w, double d, double h, JagTriangle3D tri[] )
{
	// top
	tri[0].x1 = w; tri[0].y1 = d; tri[0].z1 = h;
	tri[0].x2 = w; tri[0].y2 = -d; tri[0].z2 = h;
	tri[0].x3 = -w; tri[0].y3 = d; tri[0].z3 = h;

	// right
	tri[1].x1 = w; tri[1].y1 = d; tri[1].z1 = h;
	tri[1].x2 = w; tri[1].y2 = -d; tri[1].z2 = h;
	tri[1].x3 = w; tri[1].y3 = d; tri[1].z3 = -h;

	// bottom
	tri[2].x1 = w; tri[2].y1 = d; tri[2].z1 = -h;
	tri[2].x2 = w; tri[2].y2 = -d; tri[2].z2 = -h;
	tri[2].x3 = -w; tri[2].y3 = d; tri[2].z3 = -h;

	// left
	tri[3].x1 = -w; tri[3].y1 = d; tri[3].z1 = h;
	tri[3].x2 = -w; tri[3].y2 = -d; tri[3].z2 = h;
	tri[3].x3 = -w; tri[3].y3 = d; tri[3].z3 = -h;

	// front
	tri[4].x1 = w; tri[4].y1 = -d; tri[4].z1 = h;
	tri[4].x2 = -w; tri[4].y2 = -d; tri[4].z2 = h;
	tri[4].x3 = -w; tri[4].y3 = -d; tri[4].z3 = -h;

	// back
	tri[4].x1 = w; tri[4].y1 = d; tri[4].z1 = h;
	tri[4].x2 = -w; tri[4].y2 = d; tri[4].z2 = h;
	tri[4].x3 = -w; tri[4].y3 = d; tri[4].z3 = -h;
}

bool JagGeo::bound3DLineBoxDisjoint( double x10, double y10, double z10,
									double x20, double y20, double z20,
									double x0, double y0, double z0, double w, double d, double h )
{
	// check max
	double mxd = jagMax(w,d,h);
	double xmax = jagmax(x10,x20);
	if ( xmax < x0-mxd ) return true;

	double ymax = jagmax(y10,y20);
	if ( ymax < y0-mxd ) return true;

	double zmax = jagmax(z10,z20);
	if ( zmax < z0-mxd ) return true;


	// check min
	double xmin = jagmin(x10,x20);
	if ( xmin > x0+mxd ) return true;

	double ymin = jagmin(y10,y20);
	if ( ymin > y0+mxd ) return true;

	double zmin = jagmin(z10,z20);
	if ( ymin > z0+mxd ) return true;

	return false;
}

bool JagGeo::bound2DLineBoxDisjoint( double x10, double y10, double x20, double y20, 
									double x0, double y0, double w, double h )
{

	// check max
	double mxd = jagmax(w,h);
	double xmax = jagmax(x10,x20);
	if ( xmax < x0-mxd ) return true;

	double ymax = jagmax(y10,y20);
	if ( ymax < y0-mxd ) return true;

	// check min
	double xmin = jagmin(x10,x20);
	if ( xmin > x0+mxd ) return true;

	double ymin = jagmin(y10,y20);
	if ( ymin > y0+mxd ) return true;

	return false;
}



// return 0: disjoint, no touching points
//        1: tagent with one touching point
//        2: insersect by two touching points
// Normal ellipse
// Can be applied to circle: a=b=r
int JagGeo::relationLine3DNormalEllipsoid( double x0, double y0, double z0,  double x1, double y1, double z1,
										  double a, double b, double c )
{
	if ( jagmax(x1,x0) < -a ) return 0;
	if ( jagmin(x1,x0) > a ) return 0;

	if ( jagmax(y1,y0) < -b ) return 0;
	if ( jagmin(y1,y0) > b ) return 0;

	if ( jagmax(z1,z0) < -c ) return 0;
	if ( jagmin(z1,z0) > c ) return 0;


	if ( jagEQ(a, 0.0) ) return 0;
	if ( jagEQ(b, 0.0) ) return 0;
	if ( jagEQ(c, 0.0) ) return 0;

	if ( jagEQ(x0, x1) ) {
		// line x = x0;
		// (y-y0)/(y1-y0) = (z-z0)/(z1-z0)
		// ellipse
		// x*x/aa + yy/bb + zz/cc =1
		// x0*x0/aa + yy/bb + zz/cc = 1
		// e = 1.0 - x0*x0/(a*a)
		// yy / bb + zz / cc  = 1-x0^2/aa
		// yy (e*b*b) + zz/ (e*c*c) = 1
		// new b is b*sqrt(e)   new c is c*sqrt(e)
		// normal ellipse , newa, newb, line y0z0 y1z1 if they intersect
		if ( x0 < -a || x0 > a ) return 0;
		double e = sqrt(1.0 - x0*x0/(a*a) );
		double newa = b*e; double newb = c*e;
		int rel = relationLineNormalEllipse( y0, z0, y1, z1, newa, newb );
		return rel;
	}

	// https://johannesbuchner.github.io/intersection/intersection_line_ellipsoid.html
	// x = x0 + t
	// y = y0 + kt
	// z = z0 + ht
	// normally: x = x0 + (x1-x0)t
	// normally: y = y0 + (y1-y0)t
	// normally: z = z0 + (z1-z0)t
	// now T = (x1-x0)t   t = T/(x1-x0)
	// we have x = x0 + T
	// y = y0 + (y1-y0)T/(x1-x0)
	// z = z0 + (y1-z0)T/(x1-x0)
	double k = (y1-y0)/(x1-x0);
	double h = (z1-z0)/(x1-x0);
	double a2=a*a;
	double b2=b*b;
	double c2=c*c;
	double k2=k*k;
	double h2=h*h;
	double x0sq=x0*x0;
	double y0sq=y0*y0;
	double z0sq=z0*z0;

/***
x = x0 + (-pow(a, 2)*pow(b, 2)*l*z0 - pow(a, 2)*pow(c, 2)*k*y0 - pow(b, 2)*pow(c, 2)*x0 + sqrt(pow(a, 2)*pow(b, 2)*pow(c, 2)*(pow(a, 2)*pow(b, 2)*pow(l, 2) + pow(a, 2)*pow(c, 2)*pow(k, 2) - pow(a, 2)*pow(k, 2)*pow(z0, 2) + 2*pow(a, 2)*k*l*y0*z0 - pow(a, 2)*pow(l, 2)*pow(y0, 2) + pow(b, 2)*pow(c, 2) - pow(b, 2)*pow(l, 2)*pow(x0, 2) + 2*pow(b, 2)*l*x0*z0 - pow(b, 2)*pow(z0, 2) - pow(c, 2)*pow(k, 2)*pow(x0, 2) + 2*pow(c, 2)*k*x0*y0 - pow(c, 2)*pow(y0, 2))))/(pow(a, 2)*pow(b, 2)*pow(l, 2) + pow(a, 2)*pow(c, 2)*pow(k, 2) + pow(b, 2)*pow(c, 2));
y = k*(-pow(a, 2)*pow(b, 2)*l*z0 - pow(a, 2)*pow(c, 2)*k*y0 - pow(b, 2)*pow(c, 2)*x0 + sqrt(pow(a, 2)*pow(b, 2)*pow(c, 2)*(pow(a, 2)*pow(b, 2)*pow(l, 2) + pow(a, 2)*pow(c, 2)*pow(k, 2) - pow(a, 2)*pow(k, 2)*pow(z0, 2) + 2*pow(a, 2)*k*l*y0*z0 - pow(a, 2)*pow(l, 2)*pow(y0, 2) + pow(b, 2)*pow(c, 2) - pow(b, 2)*pow(l, 2)*pow(x0, 2) + 2*pow(b, 2)*l*x0*z0 - pow(b, 2)*pow(z0, 2) - pow(c, 2)*pow(k, 2)*pow(x0, 2) + 2*pow(c, 2)*k*x0*y0 - pow(c, 2)*pow(y0, 2))))/(pow(a, 2)*pow(b, 2)*pow(l, 2) + pow(a, 2)*pow(c, 2)*pow(k, 2) + pow(b, 2)*pow(c, 2)) + y0;
z = l*(-pow(a, 2)*pow(b, 2)*l*z0 - pow(a, 2)*pow(c, 2)*k*y0 - pow(b, 2)*pow(c, 2)*x0 + sqrt(pow(a, 2)*pow(b, 2)*pow(c, 2)*(pow(a, 2)*pow(b, 2)*pow(l, 2) + pow(a, 2)*pow(c, 2)*pow(k, 2) - pow(a, 2)*pow(k, 2)*pow(z0, 2) + 2*pow(a, 2)*k*l*y0*z0 - pow(a, 2)*pow(l, 2)*pow(y0, 2) + pow(b, 2)*pow(c, 2) - pow(b, 2)*pow(l, 2)*pow(x0, 2) + 2*pow(b, 2)*l*x0*z0 - pow(b, 2)*pow(z0, 2) - pow(c, 2)*pow(k, 2)*pow(x0, 2) + 2*pow(c, 2)*k*x0*y0 - pow(c, 2)*pow(y0, 2))))/(pow(a, 2)*pow(b, 2)*pow(l, 2) + pow(a, 2)*pow(c, 2)*pow(k, 2) + pow(b, 2)*pow(c, 2)) + z0;
x = x0 - (pow(a, 2)*pow(b, 2)*l*z0 + pow(a, 2)*pow(c, 2)*k*y0 + pow(b, 2)*pow(c, 2)*x0 + sqrt(pow(a, 2)*pow(b, 2)*pow(c, 2)*(pow(a, 2)*pow(b, 2)*pow(l, 2) + pow(a, 2)*pow(c, 2)*pow(k, 2) - pow(a, 2)*pow(k, 2)*pow(z0, 2) + 2*pow(a, 2)*k*l*y0*z0 - pow(a, 2)*pow(l, 2)*pow(y0, 2) + pow(b, 2)*pow(c, 2) - pow(b, 2)*pow(l, 2)*pow(x0, 2) + 2*pow(b, 2)*l*x0*z0 - pow(b, 2)*pow(z0, 2) - pow(c, 2)*pow(k, 2)*pow(x0, 2) + 2*pow(c, 2)*k*x0*y0 - pow(c, 2)*pow(y0, 2))))/(pow(a, 2)*pow(b, 2)*pow(l, 2) + pow(a, 2)*pow(c, 2)*pow(k, 2) + pow(b, 2)*pow(c, 2));
y = -k*(pow(a, 2)*pow(b, 2)*l*z0 + pow(a, 2)*pow(c, 2)*k*y0 + pow(b, 2)*pow(c, 2)*x0 + sqrt(pow(a, 2)*pow(b, 2)*pow(c, 2)*(pow(a, 2)*pow(b, 2)*pow(l, 2) + pow(a, 2)*pow(c, 2)*pow(k, 2) - pow(a, 2)*pow(k, 2)*pow(z0, 2) + 2*pow(a, 2)*k*l*y0*z0 - pow(a, 2)*pow(l, 2)*pow(y0, 2) + pow(b, 2)*pow(c, 2) - pow(b, 2)*pow(l, 2)*pow(x0, 2) + 2*pow(b, 2)*l*x0*z0 - pow(b, 2)*pow(z0, 2) - pow(c, 2)*pow(k, 2)*pow(x0, 2) + 2*pow(c, 2)*k*x0*y0 - pow(c, 2)*pow(y0, 2))))/(pow(a, 2)*pow(b, 2)*pow(l, 2) + pow(a, 2)*pow(c, 2)*pow(k, 2) + pow(b, 2)*pow(c, 2)) + y0;
z = -l*(pow(a, 2)*pow(b, 2)*l*z0 + pow(a, 2)*pow(c, 2)*k*y0 + pow(b, 2)*pow(c, 2)*x0 + sqrt(pow(a, 2)*pow(b, 2)*pow(c, 2)*(pow(a, 2)*pow(b, 2)*pow(l, 2) + pow(a, 2)*pow(c, 2)*pow(k, 2) - pow(a, 2)*pow(k, 2)*pow(z0, 2) + 2*pow(a, 2)*k*l*y0*z0 - pow(a, 2)*pow(l, 2)*pow(y0, 2) + pow(b, 2)*pow(c, 2) - pow(b, 2)*pow(l, 2)*pow(x0, 2) + 2*pow(b, 2)*l*x0*z0 - pow(b, 2)*pow(z0, 2) - pow(c, 2)*pow(k, 2)*pow(x0, 2) + 2*pow(c, 2)*k*x0*y0 - pow(c, 2)*pow(y0, 2))))/(pow(a, 2)*pow(b, 2)*pow(l, 2) + pow(a, 2)*pow(c, 2)*pow(k, 2) + pow(b, 2)*pow(c, 2)) + z0;

*****/
/**********
double sx1 = x0 + (-a2*b2*h*z0 - a2*c2*k*y0 - b2*c2*x0 + sqrt(a2*b2*c2*(a2*b2*h2 + a2*c2*k2 - a2*k2*z0sq 
			+ 2*a2*k*h*y0*z0 - a2*h2*y0sq + b2*c2 - b2*h2*x0sq + 2*b2*h*x0*z0 - b2*z0sq - c2*k2*x0sq 
			+ 2*c2*k*x0*y0 - c2*y0sq)))/(a2*b2*h2 + a2*c2*k2 + b2*c2);

double sy1 = k*(-a2*b2*h*z0 - a2*c2*k*y0 - b2*c2*x0 + sqrt(a2*b2*c2*(a2*b2*h2 + a2*c2*k2 - a2*k2*z0sq 
			+ 2*a2*k*h*y0*z0 - a2*h2*y0sq + b2*c2 - b2*h2*x0sq + 2*b2*h*x0*z0 - b2*z0sq - c2*k2*x0sq 
			+ 2*c2*k*x0*y0 - c2*y0sq)))/(a2*b2*h2 + a2*c2*k2 + b2*c2) + y0;

double sz1 = h*(-a2*b2*h*z0 - a2*c2*k*y0 - b2*c2*x0 + sqrt(a2*b2*c2*(a2*b2*h2 + a2*c2*k2 - a2*k2*z0sq 
			+ 2*a2*k*h*y0*z0 - a2*h2*y0sq + b2*c2 - b2*h2*x0sq + 2*b2*h*x0*z0 - b2*z0sq - c2*k2*x0sq 
			+ 2*c2*k*x0*y0 - c2*y0sq)))/(a2*b2*h2 + a2*c2*k2 + b2*c2) + z0;


double sx2 = x0 - (a2*b2*h*z0 + a2*c2*k*y0 + b2*c2*x0 + sqrt(a2*b2*c2*(a2*b2*h2 + a2*c2*k2 - a2*k2*z0sq 
			+ 2*a2*k*h*y0*z0 - a2*h2*y0sq + b2*c2 - b2*h2*x0sq + 2*b2*h*x0*z0 - b2*z0sq - c2*k2*x0sq 
			+ 2*c2*k*x0*y0 - c2*y0sq)))/(a2*b2*h2 + a2*c2*k2 + b2*c2);

double sy2 = -k*(a2*b2*h*z0 + a2*c2*k*y0 + b2*c2*x0 + sqrt(a2*b2*c2*(a2*b2*h2 + a2*c2*k2 - a2*k2*z0sq 
			+ 2*a2*k*h*y0*z0 - a2*h2*y0sq + b2*c2 - b2*h2*x0sq + 2*b2*h*x0*z0 - b2*z0sq - c2*k2*x0sq 
			+ 2*c2*k*x0*y0 - c2*y0sq)))/(a2*b2*h2 + a2*c2*k2 + b2*c2) + y0;

double sz2 = -h*(a2*b2*h*z0 + a2*c2*k*y0 + b2*c2*x0 + sqrt(a2*b2*c2*(a2*b2*h2 + a2*c2*k2 - a2*k2*z0sq 
			+ 2*a2*k*h*y0*z0 - a2*h2*y0sq + b2*c2 - b2*h2*x0sq + 2*b2*h*x0*z0 - b2*z0sq - c2*k2*x0sq 
			+ 2*c2*k*x0*y0 - c2*y0sq)))/(a2*b2*h2 + a2*c2*k2 + b2*c2) + z0;
**********/

	double D = a2*b2*h2 + a2*c2*k2 - a2*k2*z0sq 
			+ 2*a2*k*h*y0*z0 - a2*h2*y0sq + b2*c2 - b2*h2*x0sq + 2*b2*h*x0*z0 - b2*z0sq - c2*k2*x0sq 
			+ 2*c2*k*x0*y0 - c2*y0sq;

	if ( D < 0.0 ) return 0;

	double T1 = ( -a2*b2*h*z0 - a2*c2*k*y0 - b2*c2*x0 + sqrt(a2*b2*c2*D) )/(a2*b2*h2 + a2*c2*k2 + b2*c2);
	double T2 = ( a2*b2*h*z0 + a2*c2*k*y0 + b2*c2*x0 + sqrt(a2*b2*c2*D) )/(a2*b2*h2 + a2*c2*k2 + b2*c2);

	double s1x = x0 + T1;
	double s1y = y0 + k*T1;
	double s1z = z0 + h*T1;

	double s2x = x0 - T2;
	double s2y = y0 - k*T2;
	double s2z = z0 - h*T2;

	if ( jagEQ(D, 0.0) ) {
		// intersect tagent T1 = -T2 --> D=0
		if ( jagmax(x0,x1) <  s1x ) return 0;
		if ( jagmin(x0,x1) >  s1x ) return 0;
		if ( jagmax(y0,y1) <  s1y ) return 0;
		if ( jagmin(y0,y1) >  s1y ) return 0;
		if ( jagmax(z0,z1) <  s1z ) return 0;
		if ( jagmin(z0,z1) >  s1z ) return 0;
		return 1;
	}

	if ( jagmax(x0,x1) <  jagmin(s1x, s2x) ) return 0;
	if ( jagmin(x0,x1) >  jagmax(s1x, s2x)) return 0;
	if ( jagmax(y0,y1) <  jagmin(s1y, s2y) ) return 0;
	if ( jagmin(y0,y1) >  jagmax(s1y, s2y) ) return 0;
	if ( jagmax(z0,z1) <  jagmin(s1z, s2z) ) return 0;
	if ( jagmin(z0,z1) >  jagmax(s1z, s2z) ) return 0;

	return 2;
}

int JagGeo::relationLine3DEllipsoid( double x1, double y1, double z1, double x2, double y2, double z2,
                                                    double x0, double y0, double z0,
                                                    double a, double b, double c, double nx, double ny )
{
    double px1, py1, pz1, px2, py2, pz2;
	transform3DCoordGlobal2Local( x0, y0, z0, x1, y1, z1, nx, ny, px1, py1, pz1 );
	transform3DCoordGlobal2Local( x0, y0, z0, x2, y2, z2, nx, ny, px2, py2, pz2 );
	return relationLine3DNormalEllipsoid(px1, py1, pz1,  px2, py2, pz2, a, b, c );
}

// return 0: disjoint, no touching points
//        1: tagent with one touching point
//        2: insersect by two touching points
// Normal ellipse
// Can be applied to circle: a=b=r
// a: radius in center (1/2 radius at bottom circle)  c: height from center (1/2 real height)
int JagGeo::relationLine3DNormalCone( double x0, double y0, double z0,  double x1, double y1, double z1,
										  double a, double c )
{
	// a = b
	if ( jagmax(x1,x0) < -a ) return 0;
	if ( jagmin(x1,x0) > a ) return 0;

	if ( jagmax(y1,y0) < -a ) return 0;
	if ( jagmin(y1,y0) > a ) return 0;

	if ( jagmax(z1,z0) < -c ) return 0;
	if ( jagmin(z1,z0) > c ) return 0;


	if ( jagEQ(a, 0.0) ) return 0;
	if ( jagEQ(c, 0.0) ) return 0;


	// https://johannesbuchner.github.io/intersection/intersection_line_ellipsoid.html
	// x = x0 + t
	// y = y0 + kt
	// z = z0 + ht
	// normally: x = x0 + (x1-x0)t
	// normally: y = y0 + (y1-y0)t
	// normally: z = z0 + (z1-z0)t
	// now T = (x1-x0)t   t = T/(x1-x0)
	// we have x = x0 + T
	// y = y0 + (y1-y0)T/(x1-x0)
	// z = z0 + (y1-z0)T/(x1-x0)
	double k, h;
	if ( jagEQ( x0,x1 ) ) {
		k = y1-y0;
		h = z1-z0;
	} else {
		k = (y1-y0)/(x1-x0);
		h = (z1-z0)/(x1-x0);
	}

	// z*z * C*C = x*x + y*y
	// what is C?
	// in our coord system:
	//        /\
	//       /  \
	//      /    \
	//     /  0   \
	//    /        \
	//   /          \
	//   ------------
	// (c-z)/r = c/a   r = (c-z)a/c
	// r*r=x*x+y*y
	// x*x+y*y = r*r = (c-z)^2 * a^2 / c^2
	// Z= c-z  C = a/c
	//double Z = c-z;
	double C = a/c;

	double a2=a*a;
	// double b2=b*b;
	double c2=C*C;
	double k2=k*k;
	double h2=h*h;
	double x0sq=x0*x0;
	double y0sq=y0*y0;
	double z0sq=z0*z0;

	// https://johannesbuchner.github.io/intersection/intersection_line_cone.html
	double D = c2*k2*z0sq - 2*c2*k*h*y0*z0 + c2*h2*(x0sq + y0sq) - 2*c2*h*x0*z0 + c2*z0sq - k2*x0sq + 2*k*x0*y0 - y0sq;
	if ( D < 0.0 ) return 0; // disjoint
	double sqrtD = sqrt(D);

	double f1 = c2*h*z0 - k*y0 -x0;
	double denom1 = -c2*h2 +k2 + 1.0;
	double denom2 = c2*h2 -k2 - 1.0;
	double f2 = (f1+sqrtD)/denom1;
	double sx1 = x0 + f2;
	double sy1 = y0 + k*f2;
	double sz1 = z0 + h*f2;
	sz1 = c - sz1; // convert to our z-axis

	if ( jagEQ(D, 0.0) ) {
		// intersect tagent T1 = -T2 --> D=0
		if ( jagmax(x0,x1) <  sx1 ) return 0;
		if ( jagmin(x0,x1) >  sx1 ) return 0;
		if ( jagmax(y0,y1) <  sy1 ) return 0;
		if ( jagmin(y0,y1) >  sy1 ) return 0;
		if ( jagmax(z0,z1) <  sz1 ) return 0;
		if ( jagmin(z0,z1) >  sz1 ) return 0;
		return 1;
	}

	f2 = (-f1+sqrtD)/denom2;
	double sx2 = x0 + f2;
	double sy2 = y0 + k*f2;
	double sz2 = z0 + h*f2;
	sz2 = c - sz2; // convert to our z-axis

	if ( jagmax(x0,x1) <  jagmin(sx1, sx2) ) return 0;
	if ( jagmin(x0,x1) >  jagmax(sx1, sx2)) return 0;
	if ( jagmax(y0,y1) <  jagmin(sy1, sy2) ) return 0;
	if ( jagmin(y0,y1) >  jagmax(sy1, sy2) ) return 0;
	if ( jagmax(z0,z1) <  jagmin(sz1, sz2) ) return 0;
	if ( jagmin(z0,z1) >  jagmax(sz1, sz2) ) return 0;

	return 2;
}


// return 0: disjoint, no touching points
//        1: tagent with one touching point
//        2: insersect by two touching points
// Normal ellipse
// Can be applied to circle: a=b=r
// a: radius in center (1/2 radius at bottom circle)  c: height from center (1/2 real height)
int JagGeo::relationLine3DNormalCylinder( double x0, double y0, double z0,  double x1, double y1, double z1,
										  double a, double b, double c )
{
	// a = b
	if ( jagmax(x1,x0) < -a ) return 0;
	if ( jagmin(x1,x0) > a ) return 0;

	if ( jagmax(y1,y0) < -b ) return 0;
	if ( jagmin(y1,y0) > b ) return 0;

	if ( jagmax(z1,z0) < -c ) return 0;
	if ( jagmin(z1,z0) > c ) return 0;


	if ( jagEQ(a, 0.0) ) return 0;
	if ( jagEQ(b, 0.0) ) return 0;
	if ( jagEQ(c, 0.0) ) return 0;


	// https://johannesbuchner.github.io/intersection/intersection_line_cylinder.html
	// x = x0 + t
	// y = y0 + kt
	// z = z0 + ht
	// normally: x = x0 + (x1-x0)t
	// normally: y = y0 + (y1-y0)t
	// normally: z = z0 + (z1-z0)t
	// now T = (x1-x0)t   t = T/(x1-x0)
	// we have x = x0 + T
	// y = y0 + (y1-y0)T/(x1-x0)
	// z = z0 + (y1-z0)T/(x1-x0)
	double k, h;
	if ( jagEQ( x0,x1 ) ) {
		k = y1-y0;
		h = z1-z0;
	} else {
		k = (y1-y0)/(x1-x0);
		h = (z1-z0)/(x1-x0);
	}

	double a2=a*a;
	double b2=b*b;
	double k2=k*k;
	double h2=h*h;

	double x0sq=x0*x0;
	double y0sq=y0*y0;

	double D = a2*b2*(a2*k2+b2-k2*x0sq+2*k*x0*y0-y0sq);
	if ( D < 0.0 ) return 0; // disjoint
	double sqrtD = sqrt(D);
	double denom = a2*k2+b2;

	double f1 = a2*k*y0 + b2*x0;
	double f2 = (-f1 + sqrtD )/denom;
	double sx1 = x0 + f2;
	double sy1 = y0 + k*f2;
	double sz1 = z0 + h*f2;

	if ( jagEQ(D, 0.0) ) {
		// intersect tagent T1 = -T2 --> D=0
		if ( jagmax(x0,x1) <  sx1 ) return 0;
		if ( jagmin(x0,x1) >  sx1 ) return 0;
		if ( jagmax(y0,y1) <  sy1 ) return 0;
		if ( jagmin(y0,y1) >  sy1 ) return 0;
		if ( jagmax(z0,z1) <  sz1 ) return 0;
		if ( jagmin(z0,z1) >  sz1 ) return 0;
		return 1;
	}

	f2 = (f1+sqrtD)/denom;
	double sx2 = x0 + f2;
	double sy2 = y0 + k*f2;
	double sz2 = z0 + h*f2;

	if ( jagmax(x0,x1) <  jagmin(sx1, sx2) ) return 0;
	if ( jagmin(x0,x1) >  jagmax(sx1, sx2)) return 0;
	if ( jagmax(y0,y1) <  jagmin(sy1, sy2) ) return 0;
	if ( jagmin(y0,y1) >  jagmax(sy1, sy2) ) return 0;
	if ( jagmax(z0,z1) <  jagmin(sz1, sz2) ) return 0;
	if ( jagmin(z0,z1) >  jagmax(sz1, sz2) ) return 0;

	return 2;
}

int JagGeo::relationLine3DCone( double x1, double y1, double z1, double x2, double y2, double z2,
                                double x0, double y0, double z0,
                                double a, double c, double nx, double ny )
{
    double px1, py1, pz1, px2, py2, pz2;
	transform3DCoordGlobal2Local( x0, y0, z0, x1, y1, z1, nx, ny, px1, py1, pz1 );
	transform3DCoordGlobal2Local( x0, y0, z0, x2, y2, z2, nx, ny, px2, py2, pz2 );
	return relationLine3DNormalCone(px1, py1, pz1,  px2, py2, pz2, a, c );
}

int JagGeo::relationLine3DCylinder( double x1, double y1, double z1, double x2, double y2, double z2,
                                double x0, double y0, double z0,
                                double a, double b, double c, double nx, double ny )
{
    double px1, py1, pz1, px2, py2, pz2;
	transform3DCoordGlobal2Local( x0, y0, z0, x1, y1, z1, nx, ny, px1, py1, pz1 );
	transform3DCoordGlobal2Local( x0, y0, z0, x2, y2, z2, nx, ny, px2, py2, pz2 );
	return relationLine3DNormalCylinder(px1, py1, pz1,  px2, py2, pz2, a, b, c );
}

void JagGeo::triangle3DNormal( double x1, double y1, double z1, double x2, double y2, double z2,
								 double x3, double y3, double z3, double &nx, double &ny )
{
	if ( jagEQ(x1,x2) && jagEQ(y1,y2) && jagEQ(z1,z2)
			&& jagEQ(x1,x3) && jagEQ(y1,y3) && jagEQ(z1,z3)
	     ) {

		nx = 0.0; ny=0.0;
		return;
	}

	double NX = (y2-y1)*(z3-z1) - (z2-z1)*(y3-y1);
	double NY = (z2-z1)*(x3-x1) - (x2-x1)*(z3-z1);
	double NZ = (x2-x1)*(y3-y1) - (y2-y1)*(x3-x1);
	double SQ = sqrt(NX*NX + NY*NY + NZ*NZ);
	nx = NX/SQ;  ny=NY/SQ;
}


double JagGeo::distancePoint3DToPlane( double x, double y, double z, double A, double B, double C, double D )
{
	return fabs( A*x + B*y + C*z + D)/sqrt(A*A + B*B + C*C);
}

void JagGeo::planeABCDFromNormal( double x0, double y0, double z0, double nx, double ny,
								double &A, double &B, double &C, double &D )
{
	//nx*(x-x0) + ny*(y-y0) + nz*(z-z0) = 0;
	// nx*x - nx*x0 + ny*y - ny*y + nz*z - nz*z0 = 0;
	// nx*x + ny*y + nz*z - (nx*x0+ny*y0+nz*z0) = 0;
	double nz = sqrt( 1.0 - nx*nx - ny*ny );
	A=nx;
	B=ny;
	C=nz;
	D =-(nx*x0+ny*y0+nz*z0);

	if ( A < 0.0 ) { A = -A; B = -B; C = -C; D = -D; }
}

// plane: Ax + By + Cz + D = 0;
// ellipsoid  x^2/a^2 + y^2/b^2 + z^2/c^2 = 1
bool JagGeo::planeIntersectNormalEllipsoid( double A, double B, double C, double D, double a,  double b,  double c )
{
	if ( jagIsZero(A)  && jagIsZero(B) && jagIsZero(C) &&
	     jagIsZero(a)  && jagIsZero(b) && jagIsZero(c)    ) {
		 return false;
	}

	double a2 = a*a;
	double b2 = b*b;
	double c2 = c*c;

	double t = 1.0/sqrt( A*A*a2 + B*B*b2 + C*C*c2 ); 
	double x1 = -A*a2*t;
	double y1 = -B*b2*t;
	double z1 = -C*c2*t;

	double D1 = (A*x1 + B*y1 + C*z1 );
	double D2 = -D1;
	double minD = jagmin(D1, D2);
	double maxD = jagmax(D1, D2);
	if ( jagLE( D, maxD) && jagGE(D, minD) ) {
		return true;
	}

	return false;
}


void JagGeo::triangle3DABCD( double x1, double y1, double z1, double x2, double y2, double z2,
 							 double x3, double y3, double z3, double &A, double &B, double &C, double &D )
{
	A = (y2-y1)*(z3-z1) - (y3-y1)*(z2-z1);
	B = (z2-z1)*(x3-x1) - (z3-z1)*(x2-x1);
	C = (x2-x1)*(y3-y1) - (x3-x1)*(y2-y1);
	D = -(A* x1 + B* y1 + C* z1 );
	if ( A < 0.0 ) { A = -A; B = -B; C = -C; D = -D; }
}

bool JagGeo::planeIntersectNormalCone( double A, double B, double C, double D, double R,  double h )
{
	if ( jagLE(h, 0.0) || jagLE(R, 0.0) ) return false;
	double c = R/h;
	double c2 = c*c;
	double A2 = A*A;
	double B2 = B*B;
	double C2 = C*C;
	double  denom = A2 + B2 - C2/c2;

	if ( jagLE( denom, 0.0) ) return false;
	if ( jagIsZero(A) && jagIsZero(B) ) {
		 if (  jagIsZero(C) ) return false;
		// horizontal flat surface
		if ( -D/C > h || -D/C < -h ) return false;
		return true;
	}

	// c2= c*c
	// cone surface equation: x^2+y^2 = c^2(h-z)^2   c=R/h
	// -h <= z <= h  --> 2h >= h-z >= 0
	// x^2+y^2 >= 0
	// x^2+y^2 <= 4* c2* h2  // a ringle wihg radius ch and 2ch  (0, 2R)
	// Cz = -(Ax+By+D)
	// -h  <= z <= h       -Ch <= Cz <= Ch
	// Ch >= -Cz >= -Ch
	// -Ch <= Ax + By +D <= Ch
	// -Ch-D <= Ax + By <= Ch -D
	double d2 = fabs(C*h - D);
	double d1 = fabs(C*h + D);
	double mind = jagmin(d2, d1 );
	double dist2 = mind*mind/(A2+B2); 
	if ( dist2 > 2.0*R ) {
		return false;
	}
	return true;
}

bool JagGeo::doAllNearby( const AbaxDataString& mark1, const AbaxDataString &colType1, int srid1, const JagStrSplit &sp1,
                         const AbaxDataString& mark2,  const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, 
						 const AbaxDataString &carg )
{
	prt(("s0233 doAllNearby srid1=%d srid2=%d carg=[%s]\n", srid1, srid2, carg.c_str() ));

	int dim1 = getDimension( colType1 ); 
	int dim2 = getDimension( colType2 ); 
	if ( dim1 != dim2 ) {
		prt(("s2039 dim diff\n" ));
		return false;
	}

	if ( srid1 != srid2 ) {
		prt(("s2039 srid diff srid1=%d srid2=%d\n", srid1, srid2 ));
		return false;
	}

	double r = jagatof( carg.c_str() );
	double px,py,pz, x,y,z, R1x, R1y, R1z, R2x, R2y, R2z;
	getCoordAvg( colType1, sp1, px,py,pz, R1x, R1y, R1z );
	getCoordAvg( colType2, sp2, x,y,z, R2x, R2y, R2z );

	if ( 0 == srid1 ) {
    	if ( 2 == dim1 ) {
    		if ( bound2DDisjoint( px, py, R1x,R1y,  x, y, R2x+r,R2y+r ) ) {
    			prt(("s0291 bound2DDisjoint true\n" ));
    			return false;
    		}
    	} else {
    		if ( bound3DDisjoint( px, py, pz, R1x,R1y,R1z,  x, y, z, R2x+r,R2y+r,R2z+r ) ) {
    			prt(("s0231 bound3DDisjoint true\n" ));
    			return false;
    		}
    	}
	} else if ( JAG_GEO_WGS84 == srid1 ) {
    	if ( 2 == dim1 ) {
			double lon = meterToLon( srid1, r, x, y );
			double lat = meterToLat( srid1, r, x, y );
    		if ( bound2DDisjoint( px, py, R1x,R1y,  x, y, R2x+lon,R2y+lat ) ) {
    			prt(("s0291 bound2DDisjoint true\n" ));
    			return false;
    		}
    	} else {
    		if ( bound3DDisjoint( px, py, pz, R1x,R1y,R1z,  x, y, z, R2x+r,R2y+r,R2z+r ) ) {
    			prt(("s0231 bound3DDisjoint true\n" ));
    			return false;
    		}
    	}
	}

	double dist;
	if ( 2 == dim1 ) {
		dist = distance( px,py, x,y, srid1 );
		if ( jagLE( dist, r ) ) return true; 
	} else if ( 3 == dim1 ) {
		dist = distance( px,py,pz, x,y,z, srid1 );
		if ( jagLE( dist, r ) ) return true; 
	}
	prt(("s3009 dist=[%f] r=[%f]\n", dist, r ));
	
	return false;
}

AbaxDataString JagGeo::getTypeStr( const AbaxDataString& colType )
{
	AbaxDataString t;
	if ( colType == JAG_C_COL_TYPE_POINT ) {
		t = "Point";
	} else if ( colType == JAG_C_COL_TYPE_LINE ) {
		t = "Line";
	} else if ( colType == JAG_C_COL_TYPE_LINESTRING ) {
		t = "LineString";
	} else if ( colType == JAG_C_COL_TYPE_MULTILINESTRING ) {
		t = "MultiLineString";
	} else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		t = "MultiPolygon";
	} else if ( colType == JAG_C_COL_TYPE_MULTIPOINT ) {
		t = "MultiPoint";
	} else if ( colType == JAG_C_COL_TYPE_POLYGON ) {
		t = "Polygon";
	} else if ( colType == JAG_C_COL_TYPE_CIRCLE ) {
		t = "Circle";
	} else if ( colType == JAG_C_COL_TYPE_SQUARE ) {
		t = "Square";
	} else if ( colType == JAG_C_COL_TYPE_RECTANGLE ) {
		t = "Rectangle";
	} else if ( colType == JAG_C_COL_TYPE_TRIANGLE ) {
		t = "Triangle";
	} else if ( colType == JAG_C_COL_TYPE_ELLIPSE ) {
		t = "Ellipse";
	} else if ( colType == JAG_C_COL_TYPE_POINT3D ) {
		t = "Point3D";
	} else if ( colType == JAG_C_COL_TYPE_LINE3D ) {
		t = "Line3D";
	} else if ( colType == JAG_C_COL_TYPE_LINESTRING3D ) {
		t = "LineString3D";
	} else if ( colType == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		t = "MultiLineString3D";
	} else if ( colType == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		t = "MultiPoint3D";
	} else if ( colType == JAG_C_COL_TYPE_POLYGON3D ) {
		t = "Polygon3D";
	} else if ( colType == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		t = "MultiPolygon3D";
	} else if ( colType == JAG_C_COL_TYPE_CIRCLE3D ) {
		t = "Circle3D";
	} else if ( colType == JAG_C_COL_TYPE_SPHERE ) {
		t = "Sphere";
	} else if ( colType == JAG_C_COL_TYPE_SQUARE3D ) {
		t = "Square3D";
	} else if ( colType == JAG_C_COL_TYPE_CUBE ) {
		t = "Cube";
	} else if ( colType == JAG_C_COL_TYPE_RECTANGLE3D ) {
		t = "Rectangle3D";
	} else if ( colType == JAG_C_COL_TYPE_BOX  ) {
		t = "Box";
	} else if ( colType == JAG_C_COL_TYPE_TRIANGLE3D  ) {
		t = "Triangle3D";
	} else if ( colType == JAG_C_COL_TYPE_CYLINDER  ) {
		t = "Cylinder";
	} else if ( colType == JAG_C_COL_TYPE_CONE   ) {
		t = "Cone";
	} else if ( colType == JAG_C_COL_TYPE_ELLIPSE3D   ) {
		t = "Ellipse3D";
	} else if ( colType == JAG_C_COL_TYPE_ELLIPSOID ) {
		t = "Ellipsoid";
	} else {
		t = "Unknown";
	}
	return t;
}

int JagGeo::getDimension( const AbaxDataString& colType )
{
	if ( colType == JAG_C_COL_TYPE_POINT 
		|| colType == JAG_C_COL_TYPE_LINE 
		|| colType == JAG_C_COL_TYPE_LINESTRING
		|| colType == JAG_C_COL_TYPE_MULTILINESTRING
		|| colType == JAG_C_COL_TYPE_MULTIPOLYGON
		|| colType == JAG_C_COL_TYPE_MULTIPOINT
		|| colType == JAG_C_COL_TYPE_POLYGON
		|| colType == JAG_C_COL_TYPE_CIRCLE
		|| colType == JAG_C_COL_TYPE_SQUARE
		|| colType == JAG_C_COL_TYPE_RECTANGLE
		|| colType == JAG_C_COL_TYPE_TRIANGLE
		|| colType == JAG_C_COL_TYPE_ELLIPSE
		 ) {
		 return 2;
	 } else if (  colType == JAG_C_COL_TYPE_POINT3D
	 			 ||  colType == JAG_C_COL_TYPE_LINE3D
	 			 ||  colType == JAG_C_COL_TYPE_LINESTRING3D
	 			 ||  colType == JAG_C_COL_TYPE_MULTILINESTRING3D
				 || colType == JAG_C_COL_TYPE_MULTIPOINT3D
	 			 ||  colType == JAG_C_COL_TYPE_POLYGON3D
	 			 ||  colType == JAG_C_COL_TYPE_MULTIPOLYGON3D
	 			 ||  colType == JAG_C_COL_TYPE_CIRCLE3D
	 			 ||  colType == JAG_C_COL_TYPE_SPHERE
	 			 ||  colType == JAG_C_COL_TYPE_SQUARE3D
	 			 ||  colType == JAG_C_COL_TYPE_CUBE
	 			 ||  colType == JAG_C_COL_TYPE_RECTANGLE3D
	 			 ||  colType == JAG_C_COL_TYPE_BOX
	 			 ||  colType == JAG_C_COL_TYPE_TRIANGLE3D
	 			 ||  colType == JAG_C_COL_TYPE_CYLINDER
	 			 ||  colType == JAG_C_COL_TYPE_CONE
	 			 ||  colType == JAG_C_COL_TYPE_ELLIPSE3D
	 			 ||  colType == JAG_C_COL_TYPE_ELLIPSOID
				 ) {
		 return 3;
	 } else {
	 	return 0;
	 }
}


int JagGeo::getPolyDimension( const AbaxDataString& colType )
{
	if ( colType == JAG_C_COL_TYPE_LINESTRING
	     || colType == JAG_C_COL_TYPE_MULTILINESTRING
		 || colType == JAG_C_COL_TYPE_POLYGON
		 || colType == JAG_C_COL_TYPE_MULTIPOLYGON
		 || colType == JAG_C_COL_TYPE_MULTIPOINT
		 ) {
		 return 2;
	 } else if (  colType == JAG_C_COL_TYPE_LINESTRING3D
	     		 || colType == JAG_C_COL_TYPE_MULTILINESTRING3D
	 			 ||  colType == JAG_C_COL_TYPE_POLYGON3D
	 			 ||  colType == JAG_C_COL_TYPE_MULTIPOLYGON3D
	 			 ||  colType == JAG_C_COL_TYPE_MULTIPOINT3D
				 ) {
		 return 3;
	 } else {
	 	return 0;
	 }
}

void JagGeo::getCoordAvg( const AbaxDataString &colType, const JagStrSplit &sp, double &x, double &y, double &z, 
							double &Rx, double &Ry, double &Rz )
{
	double px1 = jagatof( sp[0].c_str() );
	double py1 = jagatof( sp[1].c_str() );
	double pz1 = jagatof( sp[2].c_str() );
	x = px1;
	y = py1;
	z = pz1;
	Rx = Ry = Rz = 0.0;

	if ( colType == JAG_C_COL_TYPE_LINE ) {
		double px2 = jagatof( sp[2].c_str() );
		double py2 = jagatof( sp[3].c_str() );
		x = (px1+px2)/2.0;
		y = (py1+py2)/2.0;
		Rx = jagmax( fabs(x-px1), fabs(x-px2) );
		Ry = jagmax( fabs(y-py1), fabs(y-py2) );
	} else if ( colType == JAG_C_COL_TYPE_LINE3D ) {
		double px2 = jagatof( sp[3].c_str() );
		double py2 = jagatof( sp[4].c_str() );
		double pz2 = jagatof( sp[5].c_str() );
		double px3 = jagatof( sp[6].c_str() );
		double py3 = jagatof( sp[7].c_str() );
		double pz3 = jagatof( sp[8].c_str() );
		x = (px1+px2+px3)/3.0;
		y = (py1+py2+py3)/3.0;
		z = (pz1+pz2+pz3)/3.0;
		Rx = jagMax( fabs(x-px1), fabs(x-px2), fabs(x-px3) );
		Ry = jagMax( fabs(y-py1), fabs(y-py2), fabs(y-py3) );
		Rz = jagMax( fabs(z-pz1), fabs(z-pz2), fabs(z-pz3) );
	} else if ( colType == JAG_C_COL_TYPE_TRIANGLE ) {
		double px2 = jagatof( sp[2].c_str() );
		double py2 = jagatof( sp[3].c_str() );
		double px3 = jagatof( sp[4].c_str() );
		double py3 = jagatof( sp[5].c_str() );
		x = (px1+px2+px3)/3.0;
		y = (py1+py2+py3)/3.0;
		Rx = jagMax( fabs(x-px1), fabs(x-px2), fabs(x-px3) );
		Ry = jagMax( fabs(y-py1), fabs(y-py2), fabs(y-py3) );
	} else if ( colType == JAG_C_COL_TYPE_TRIANGLE3D ) {
		double px2 = jagatof( sp[3].c_str() );
		double py2 = jagatof( sp[4].c_str() );
		double pz2 = jagatof( sp[5].c_str() );
		double px3 = jagatof( sp[6].c_str() );
		double py3 = jagatof( sp[7].c_str() );
		double pz3 = jagatof( sp[8].c_str() );
		x = (px1+px2+px3)/3.0;
		y = (py1+py2+py3)/3.0;
		z = (pz1+pz2+pz3)/3.0;
		Rx = jagMax( fabs(x-px1), fabs(x-px2), fabs(x-px3) );
		Ry = jagMax( fabs(y-py1), fabs(y-py2), fabs(y-py3) );
		Rz = jagMax( fabs(z-pz1), fabs(z-pz2), fabs(z-pz3) );
	} else if ( colType == JAG_C_COL_TYPE_CUBE || colType == JAG_C_COL_TYPE_SPHERE || colType == JAG_C_COL_TYPE_CIRCLE3D ) {
		Rx = jagatof( sp[3].c_str() );
		Rz = Ry = Rx;
	} else if ( colType == JAG_C_COL_TYPE_BOX || colType == JAG_C_COL_TYPE_ELLIPSOID ) {
		Rx = jagatof( sp[3].c_str() );
		Ry = jagatof( sp[4].c_str() );
		Rz = jagatof( sp[5].c_str() );
	} else if ( colType == JAG_C_COL_TYPE_CONE || colType ==  JAG_C_COL_TYPE_RECTANGLE3D ) {
		Rx = Ry = jagatof( sp[3].c_str() );
		Rz = jagatof( sp[4].c_str() );
	} else if ( colType == JAG_C_COL_TYPE_CIRCLE || colType == JAG_C_COL_TYPE_SQUARE ) {
		Rx = Ry = jagatof( sp[2].c_str() );
	} else if ( colType == JAG_C_COL_TYPE_RECTANGLE ||  colType == JAG_C_COL_TYPE_ELLIPSE ) {
		Rx = jagatof( sp[2].c_str() );
		Ry = jagatof( sp[3].c_str() );
	}
}

bool JagGeo::bound2DTriangleDisjoint( double px1, double py1, double px2, double py2, double px3, double py3,
							  double px0, double py0, double w, double h )
{
	double mxd = jagmax(w,h);
	double m = jagMin(px1, px2, px3 );
	if ( px0 < m-mxd ) return true;

	m = jagMin(py1, py2, py3 );
	if ( py0 < m-mxd ) return true;

	m = jagMax(px1, px2, px3 );
	if ( px0 > m+mxd ) return true;

	m = jagMax(py1, py2, py3 );
	if ( px0 > m+mxd ) return true;

	return false;
}

bool JagGeo::bound3DTriangleDisjoint( double px1, double py1, double pz1, double px2, double py2, double pz2,
									  double px3, double py3, double pz3,
							  double px0, double py0, double pz0, double w, double d, double h )
{
	double mxd = jagMax(w,d,h);
	double m = jagMin(px1, px2, px3 );
	if ( px0 < m-mxd ) return true;

	m = jagMin(py1, py2, py3 );
	if ( py0 < m-mxd ) return true;

	m = jagMin(pz1, pz2, pz3 );
	if ( pz0 < m-mxd ) return true;



	m = jagMax(px1, px2, px3 );
	if ( px0 > m+mxd ) return true;

	m = jagMax(py1, py2, py3 );
	if ( py0 > m+mxd ) return true;

	m = jagMax(pz1, pz2, pz3 );
	if ( pz0 > m+mxd ) return true;

	return false;
}

void JagGeo::triangleRegion( double x1, double y1, double x2, double y2, double x3, double y3,
							 double &x0, double &y0, double &Rx, double &Ry )
{
	x0 = (x1+x2+x3)/3.0;
	y0 = (y1+y2+y3)/3.0;

	double m, R1, R2;

	m = jagMin(x1,x2,x3);
	R1 = fabs(x0-m);
	m = jagMax(x1,x2,x3);
	R2 = fabs(m-x0);
	Rx = jagmax(R1, R2 );

	m = jagMin(y1,y2,y3);
	R1 = fabs(y0-m);
	m = jagMax(y1,y2,y3);
	R2 = fabs(m-y0);
	Ry = jagmax(R1, R2 );

}

void JagGeo::triangle3DRegion( double x1, double y1, double z1,
                               double x2, double y2, double z2,
                               double x3, double y3, double z3,
                               double &x0, double &y0, double &z0, double &Rx, double &Ry, double &Rz )
{
	x0 = (x1+x2+x3)/3.0;
	y0 = (y1+y2+y3)/3.0;
	z0 = (z1+z2+z3)/3.0;

	double m, R1, R2;

	m = jagMin(x1,x2,x3);
	R1 = fabs(x0-m);
	m = jagMax(x1,x2,x3);
	R2 = fabs(m-x0);
	Rx = jagmax(R1, R2 );

	m = jagMin(y1,y2,y3);
	R1 = fabs(y0-m);
	m = jagMax(y1,y2,y3);
	R2 = fabs(m-y0);
	Ry = jagmax(R1, R2 );

	m = jagMin(z1,z2,z3);
	R1 = fabs(z0-m);
	m = jagMax(z1,z2,z3);
	R2 = fabs(m-z0);
	Rz = jagmax(R1, R2 );

}

void JagGeo::boundingBoxRegion( const AbaxDataString &bbxstr, double &bbx, double &bby, double &brx, double &bry )
{
	// "xmin:ymin:zmin:xmax:ymax:zmax"
	double xmin, ymin, xmax, ymax;
	JagStrSplit s( bbxstr, ':');
	if ( s.length() == 4 ) {
		xmin = jagatof( s[0].c_str() );
		ymin = jagatof( s[1].c_str() );
		xmax = jagatof( s[2].c_str() );
		ymax = jagatof( s[3].c_str() );
	} else {
		xmin = jagatof( s[0].c_str() );
		ymin = jagatof( s[1].c_str() );
		xmax = jagatof( s[4].c_str() );
		ymax = jagatof( s[5].c_str() );
	}

	//prt(("s3376 xmin=%.2f ymin=%.2f xmax=%.2f ymax=%.2f\n", xmin, ymin, xmax, ymax ));

	bbx = ( xmin + xmax ) /2.0;
	bby = ( ymin + ymax ) /2.0;
	brx = bbx - xmin;
	bry = bby - ymin;

}


void JagGeo::boundingBox3DRegion( const AbaxDataString &bbxstr, double &bbx, double &bby, double &bbz,
								  double &brx, double &bry, double &brz )
{
	// "xmin:ymin:xmax:ymax"
	JagStrSplit s( bbxstr, ':');
	double xmin = jagatof( s[0].c_str() );
	double ymin = jagatof( s[1].c_str() );
	double zmin = jagatof( s[2].c_str() );
	double xmax = jagatof( s[3].c_str() );
	double ymax = jagatof( s[4].c_str() );
	double zmax = jagatof( s[5].c_str() );

	bbx = ( xmin + xmax ) /2.0;
	bby = ( ymin + ymax ) /2.0;
	bbz = ( zmin + zmax ) /2.0;
	brx = bbx - xmin;
	bry = bby - ymin;
	brz = bbz - zmin;
}

// M is length of sp
void JagGeo::prepareKMP( const JagStrSplit &sp, int start, int M, int *lps )
{
    int len = 0;
    lps[0] = 0; 
    int i = 1;
    while (i < M) {
        if (sp[i+start] == sp[len+start]) {
            len++;
            lps[i] = len;
            i++;
        } else {
            if (len != 0) {
                len = lps[len-1];
            } else {
                lps[i] = 0;
                i++;
            }
        }
    }
}


// if sp2 contains sp1; or if sp1 is within sp2
// return -1: if not found; index to sp2 contaning sp1
int JagGeo::KMPPointsWithin( const JagStrSplit &sp1, int start1, const JagStrSplit &sp2, int start2 )
{
	/// sp2 is txt , sp1 is pat
	if ( sp2.length()-start2==0 || sp1.length()-start1==0 ) return -1;
    int N = sp2.length()-start2;
    int M = sp1.length()-start1;
	if ( M > N ) return -1;
    int lps[M];
    JagGeo::prepareKMP(sp1, start1, M, lps);
    int i = 0, j=0;
    while (i < N) {
        if (sp1[j+start1] == sp2[i+start2]) {
            j++; i++;
        }
 
        if (j == M) {
			return i-j;
        }
 
        if (i < N  &&  sp1[j+start1] != sp2[i+start2] ) {
            if (j != 0) j = lps[j-1];
            else i = i+1;
        }
    }
	return -1;
}

#if 0
#define  MAX_LEVELS  300
template <class POINT>
int JagGeo::sortLinePoints( POINT arr[], int elements )
{
    // int  piv, beg[MAX_LEVELS], end[MAX_LEVELS], i=0, L, R, swap;
    int  beg[MAX_LEVELS], end[MAX_LEVELS], i=0, L, R, swap;
	POINT piv;
    beg[0]=0; end[0]=elements;
    while (i>=0) {
        L=beg[i]; R=end[i]-1;
        if (L<R) {
            piv=arr[L];
            while (L<R) {
            	while ( arr[R] >= piv && L<R) { 
					// if arr[R] and piv intersect, return 1
					/***
					if ( arr[R].color != piv.color ) {
						if ( lineIntersectLine( arr[R].x1,arr[R].y1,arr[R].x2,arr[R].y2, 
											    piv.x1,piv.y1,piv.x2,piv.y2 ) ) {
							prt(("e6303 %f %f %f %f   %f %f %f %f  intersect\n",
								  arr[R].x1,arr[R].y1,arr[R].x2,arr[R].y2,
								  piv.x1,piv.y1,piv.x2,piv.y2  ));
							return 1;
						}
					}
					***/
					R--; 
				}
    			if (L<R) { arr[L++]=arr[R]; }
            	while (arr[L] <= piv && L<R) { 
					// if arr[L] and piv intersect, return 1
					// if ( lineIntersectLine(  x1, y1, x2, y2, x3, y3, x4, y4  ) ) return 1;
					/***
					if ( arr[L].color != piv.color ) {
						if ( lineIntersectLine( arr[L].x1,arr[L].y1,arr[L].x2,arr[L].y2, 
							 piv.x1,piv.y1,piv.x2,piv.y2 ) ) {
							prt(("e6304 %f %f %f %f   %f %f %f %f  intersect\n",
								  arr[L].x1,arr[L].y1,arr[L].x2,arr[L].y2,
								  piv.x1,piv.y1,piv.x2,piv.y2  ));
							 return 1;
					    }
					}
					***/
					L++; 
				}
    			if (L<R) { arr[R--]=arr[L];  }
    	    }
    
            arr[L]=piv; 
			beg[i+1]=L+1; end[i+1]=end[i]; end[i++]=L; 
    
            if (end[i]-beg[i]>end[i-1]-beg[i-1]) {
            	swap=beg[i]; beg[i]=beg[i-1]; beg[i-1]=swap;
            	swap=end[i]; end[i]=end[i-1]; end[i-1]=swap; 
    	  	} 
  	  	} else {
        	i--; 
  	  	}
   }

   return 0;
}
#endif

////////////////////////////////////////// 2D orientation //////////////////////
short JagGeo::orientation(double x1, double y1, double x2, double y2, double x3, double y3 )
{
	double v = (y2 - y1) * (x3 - x2) - (x2 - x1) * (y3 - y2);
	if ( v > 0.0 ) return JAG_CLOCKWISE;
	if ( jagEQ( v, 0.0) ) return JAG_COLINEAR;
	return JAG_COUNTERCLOCKWISE;
}

// is (x y) is "above" line x2 y2 --- x3 y3
bool JagGeo::above(double x, double y, double x2, double y2, double x3, double y3 )
{
	//prt(("s7638 JagGeo::above (x=%.3f y=%.3f) x2=%.3f y2=%.3f x3=%3f y3=%.3f\n", x,y,x2,y2,x3,y3 ));
	bool b1 = isNull(x,y);
	bool b2 = isNull(x2,y2,x3,y3);
	if ( b1 && b2 ) return false;
	if ( b1 ) return false;
	if ( b2 ) return true;

	double v = (y2 - y) * (x3 - x2) - (x2 - x) * (y3 - y2);
	if ( v < 0.0 ) return true;
	return false;
}
// is (x y) is "above" or same (colinear) line x2 y2 --- x3 y3
bool JagGeo::aboveOrSame(double x, double y, double x2, double y2, double x3, double y3 )
{
	//prt(("s7634 JagGeo::aboveorsame (x=%.3f y=%.3f) x2=%.3f y2=%.3f x3=%3f y3=%.3f\n", x,y,x2,y2,x3,y3 ));
	bool b1 = isNull(x,y);
	bool b2 = isNull(x2,y2,x3,y3);
	if ( b1 && b2 ) return true;
	if ( b1 ) return false;
	if ( b2 ) return true;

	double v = (y2 - y) * (x3 - x2) - (x2 - x) * (y3 - y2);
	if ( v > 0.0 ) return false;
	return true;
}

bool JagGeo::same(double x, double y, double x2, double y2, double x3, double y3 )
{
	//prt(("s7434 JagGeo::same (x=%.3f y=%.3f) x2=%.3f y2=%.3f x3=%3f y3=%.3f\n", x,y,x2,y2,x3,y3 ));
	bool b1 = isNull(x,y);
	bool b2 = isNull(x2,y2,x3,y3);
	if ( b1 && b2 ) return true;

	double v = (y2 - y) * (x3 - x2) - (x2 - x) * (y3 - y2);
	if ( jagEQ( v, 0.0) ) return true;
	return false;
}

bool JagGeo::below(double x, double y, double x2, double y2, double x3, double y3 )
{
	//prt(("s5434 JagGeo::below (x=%.3f y=%.3f) x2=%.3f y2=%.3f x3=%3f y3=%.3f\n", x,y,x2,y2,x3,y3 ));
	bool b1 = isNull(x,y);
	bool b2 = isNull(x2,y2,x3,y3);
	if ( b1 && b2 ) return false;
	if ( b1 ) return true;
	if ( b2 ) return false;
	double v = (y2 - y) * (x3 - x2) - (x2 - x) * (y3 - y2);
	if ( v > 0.0 ) return true;
	return false;
}

bool JagGeo::belowOrSame(double x, double y, double x2, double y2, double x3, double y3 )
{
	//prt(("s5430 JagGeo::beloworsame (x=%.3f y=%.3f) x2=%.3f y2=%.3f x3=%3f y3=%.3f\n", x,y,x2,y2,x3,y3 ));
	bool b1 = isNull(x,y);
	bool b2 = isNull(x2,y2,x3,y3);
	if ( b1 && b2 ) return true;
	if ( b1 ) return true;
	if ( b2 ) return false;

	double v = (y2 - y) * (x3 - x2) - (x2 - x) * (y3 - y2);
	if ( v < 0.0 ) return false;
	return true;
}



////////////////////////////////////////// 3D orientation //////////////////////
short JagGeo::orientation(double x1, double y1, double z1, double x2, double y2, double z2, double x3, double y3, double z3 )
{
	double v = (z2 - z1) * (x3 - x1) - (x2 - x1) * (z3 - z1);
	if ( v > 0.0 ) return JAG_CLOCKWISE; 
	if ( jagEQ( v, 0.0) ) return JAG_COLINEAR;
	return JAG_COUNTERCLOCKWISE;
}

// is (x y) is "above" line x2 y2 --- x3 y3
bool JagGeo::above(double x, double y, double z, double x2, double y2, double z2, double x3, double y3, double z3 )
{
	prt(("s2424 JagGeo::above (x=%.1f y=%.1f z=%.1f) x2=%.1f y2=%.1f z2=%.1f x3=%1f y3=%.1f z3=%.1f\n", x,y,z,x2,y2,z2,x3,y3,z3 ));
	bool b1 = isNull(x,y,z);
	bool b2 = isNull(x2,y2,z2,x3,y3,z3);
	if ( b1 && b2 ) return false;
	if ( b1 ) return false;
	if ( b2 ) return true;

	double v = (z2 - z) * (x3 - x) - (x2 - x) * (z3 - z);
	if ( v < 0.0 ) return true;
	return false;
}
// is (x y) is "above" or same (colinear) line x2 y2 --- x3 y3
bool JagGeo::aboveOrSame(double x, double y, double z, double x2, double y2, double z2, double x3, double y3, double z3 )
{
	prt(("s2428 JagGeo::aboveorsame (x=%.1f y=%.1f z=%.1f) x2=%.1f y2=%.1f z2=%.1f x3=%1f y3=%.1f z3=%.1f\n", x,y,z,x2,y2,z2,x3,y3,z3 ));
	bool b1 = isNull(x,y,z);
	bool b2 = isNull(x2,y2,z2,x3,y3,z3);
	if ( b1 && b2 ) return true;
	if ( b1 ) return false;
	if ( b2 ) return true;

	double v = (z2 - z) * (x3 - x) - (x2 - x) * (z3 - z);
	if ( v > 0.0 ) return false;
	return true;
}

bool JagGeo::same(double x, double y, double z, double x2, double y2, double z2, double x3, double y3, double z3 )
{
	prt(("s6428 JagGeo::same (x=%.1f y=%.1f z=%.1f) x2=%.1f y2=%.1f z2=%.1f x3=%1f y3=%.1f z3=%.1f\n", x,y,z,x2,y2,z2,x3,y3,z3 ));
	bool b1 = isNull(x,y,z);
	bool b2 = isNull(x2,y2,z2,x3,y3,z3);
	if ( b1 && b2 ) return true;

	double v = (z2 - z) * (x3 - x) - (x2 - x) * (z3 - z);
	if ( jagEQ( v, 0.0) ) return true;
	return false;
}

bool JagGeo::below(double x, double y, double z, double x2, double y2, double z2, double x3, double y3, double z3 )
{
	prt(("s6438 JagGeo::below (x=%.1f y=%.1f z=%.1f) x2=%.1f y2=%.1f z2=%.1f x3=%1f y3=%.1f z3=%.1f\n", x,y,z,x2,y2,z2,x3,y3,z3 ));
	bool b1 = isNull(x,y,z);
	bool b2 = isNull(x2,y2,z2,x3,y3,z3);
	if ( b1 && b2 ) return false;
	if ( b1 ) return true;
	if ( b2 ) return false;
	double v = (z2 - z) * (x3 - x) - (x2 - x) * (z3 - z);
	if ( v > 0.0 ) return true;
	return false;
}

bool JagGeo::belowOrSame(double x, double y, double z, double x2, double y2, double z2, double x3, double y3, double z3 )
{
	prt(("s6430 JagGeo::beloworsame (x=%.1f y=%.1f z=%.1f) x2=%.1f y2=%.1f z2=%.1f x3=%1f y3=%.1f z3=%.1f\n", x,y,z,x2,y2,z2,x3,y3,z3 ));
	bool b1 = isNull(x,y,z);
	bool b2 = isNull(x2,y2,z2,x3,y3,z3);
	if ( b1 && b2 ) return true;
	if ( b1 ) return true;
	if ( b2 ) return false;

	double v = (z2 - z) * (x3 - x) - (x2 - x) * (z3 - z);
	if ( v < 0.0 ) return false;
	return true;
}


////////////// JagLineSeg2D ///////////////
bool JagLineSeg2D::operator>( const JagLineSeg2D &o) const
{
	if ( JagGeo::above(x1,y1, o.x1,o.y1,o.x2,o.y2) ) return true;
	return false;
}

bool JagLineSeg2D::operator>=( const JagLineSeg2D &o) const
{
	if ( JagGeo::aboveOrSame(x1,y1, o.x1,o.y1,o.x2,o.y2) ) return true;
	return false;
}

bool JagLineSeg2D::operator<( const JagLineSeg2D &o) const
{
	if ( JagGeo::below(x1,y1, o.x1,o.y1,o.x2,o.y2) ) return true;
	return false;
}

bool JagLineSeg2D::operator<=( const JagLineSeg2D &o) const
{
	if ( JagGeo::belowOrSame(x1,y1, o.x1,o.y1,o.x2,o.y2) ) return true;
	return false;
}

bool JagLineSeg2D::operator==( const JagLineSeg2D &o) const
{
	bool b1 =  isNull(); bool b2 =  o.isNull();
	//prt(("s0284 JagLineSeg2D '==' b1=%d b2=%d\n", b1, b2 ));
	if ( b1 && b2 ) return true;
	if (  b1 || b2 ) return false;

	//prt(("s0284 JagLineSeg2D '==' b1=%d b2=%d\n", b1, b2 ));
	//prt(("s0284 x1=%.1f y1=%.1f o.x1=%.1f o.y1=%.1f o.x2=%.1f o.y2=%.1f\n", x1,y1, o.x1,o.y1,o.x2,o.y2 ));

	if ( JagGeo::same(x1,y1, o.x1,o.y1,o.x2,o.y2) ) return true;
	return false;
}
bool JagLineSeg2D::operator!=( const JagLineSeg2D &o) const
{
	bool b1 =  isNull(); bool b2 =  o.isNull();
	//prt(("s0284 JagLineSeg2D '!=' b1=%d b2=%d\n", b1, b2 ));
	if ( b1 && b2 ) return false;
	if (  b1 || b2 ) return true;

	//prt(("s1284 JagLineSeg2D '!=' b1=%d b2=%d\n", b1, b2 ));
	//prt(("s1284 x1=%.1f y1=%.1f o.x1=%.1f o.y1=%.1f o.x2=%.1f o.y2=%.1f\n", x1,y1, o.x1,o.y1,o.x2,o.y2 ));

	if ( JagGeo::same(x1,y1, o.x1,o.y1,o.x2,o.y2) ) return false;
	return true;
}

abaxint JagLineSeg2D::hashCode() const
{
	return (x1+y1+x2+y2)*4129.293/7;
}

bool JagLineSeg2D::isNull() const 
{
	if ( JagGeo::jagEQ(x1, LONG_MIN) && JagGeo::jagEQ(x2, LONG_MIN) 
		 && JagGeo::jagEQ(y1, LONG_MIN) && JagGeo::jagEQ(y2, LONG_MIN) ) {
		return true;
	}
	return false;
}

////////////// JagLineSeg3D ///////////////
bool JagLineSeg3D::operator>( const JagLineSeg3D &o) const
{
	if ( JagGeo::above(x1,y1,z1, o.x1,o.y1,o.z1,o.x2,o.y2,o.z2) ) return true;
	return false;
}

bool JagLineSeg3D::operator>=( const JagLineSeg3D &o) const
{
	if ( JagGeo::aboveOrSame(x1,y1,z1, o.x1,o.y1,o.z1,o.x2,o.y2,o.z2) ) return true;
	return false;
}

bool JagLineSeg3D::operator<( const JagLineSeg3D &o) const
{
	if ( JagGeo::below(x1,y1,z1, o.x1,o.y1,o.z1,o.x2,o.y2,o.z2) ) return true;
	return false;
}

bool JagLineSeg3D::operator<=( const JagLineSeg3D &o) const
{
	if ( JagGeo::belowOrSame(x1,y1,z1, o.x1,o.y1,o.z1,o.x2,o.y2,o.z2) ) return true;
	return false;
}

bool JagLineSeg3D::operator==( const JagLineSeg3D &o) const
{
	bool b1 =  isNull(); bool b2 =  o.isNull();
	//prt(("s0284 JagLineSeg3D '==' b1=%d b2=%d\n", b1, b2 ));
	if ( b1 && b2 ) return true;
	if (  b1 || b2 ) return false;
	if ( JagGeo::same(x1,y1,z1, o.x1,o.y1,o.z1,o.x2,o.y2,o.z2) ) return true;
	return false;
}
bool JagLineSeg3D::operator!=( const JagLineSeg3D &o) const
{
	bool b1 =  isNull(); bool b2 =  o.isNull();
	if ( b1 && b2 ) return false;
	if (  b1 || b2 ) return true;
	if ( JagGeo::same(x1,y1,z1, o.x1,o.y1,o.z1,o.x2,o.y2,o.z2) ) return false;
	return true;
}

abaxint JagLineSeg3D::hashCode() const
{
	return (x1+y1+z1+x2+y2+z2)*49.293/7;
}

bool JagLineSeg3D::isNull() const 
{
	if ( JagGeo::jagEQ(x1, LONG_MIN) && JagGeo::jagEQ(x2, LONG_MIN) 
		 && JagGeo::jagEQ(y1, LONG_MIN) && JagGeo::jagEQ(y2, LONG_MIN)
		 && JagGeo::jagEQ(z1, LONG_MIN) && JagGeo::jagEQ(z2, LONG_MIN) ) {
		return true;
	}
	return false;
}

///////////////////////////////////////////////////////////////////
bool JagGeo::isNull( double x, double y ) 
{
	if ( JagGeo::jagEQ(x, LONG_MIN) && JagGeo::jagEQ(x, LONG_MIN)  ) {
		return true;
	}
	return false;
}
bool JagGeo::isNull( double x1, double y1, double x2, double y2 ) 
{
	if ( JagGeo::jagEQ(x1, LONG_MIN) && JagGeo::jagEQ(x2, LONG_MIN) 
		 && JagGeo::jagEQ(y1, LONG_MIN) && JagGeo::jagEQ(y2, LONG_MIN) ) {
		return true;
	}
	return false;
}
bool JagGeo::isNull( double x, double y, double z ) 
{
	if ( JagGeo::jagEQ(x, LONG_MIN) && JagGeo::jagEQ(x, LONG_MIN) && JagGeo::jagEQ(z, LONG_MIN)  ) {
		return true;
	}
	return false;
}
bool JagGeo::isNull( double x1, double y1, double z1, double x2, double y2, double z2 ) 
{
	if ( JagGeo::jagEQ(x1, LONG_MIN) && JagGeo::jagEQ(x2, LONG_MIN) 
		 && JagGeo::jagEQ(y1, LONG_MIN) && JagGeo::jagEQ(y2, LONG_MIN) 
		 && JagGeo::jagEQ(z1, LONG_MIN) && JagGeo::jagEQ(z2, LONG_MIN) ) {
		return true;
	}
	return false;
}

// sp: _OJAG_=0=test.lstr.ls=LS guarantee 3 '=' signs
// str: "x:y x:y x:y ..." or "x:y:z x:y:z x:y:z ..."
AbaxDataString JagGeo::makeGeoJson( const JagStrSplit &sp, const char *str )
{
	//prt(("s3391 makeGeoJson sp[3]=[%s]\n", sp[3].c_str() ));
	//prt(("s3392 sp.print: \n" ));
	//sp.print();

	if ( sp[3] == JAG_C_COL_TYPE_LINESTRING ) {
		return makeJsonLineString("LineString", sp, str );
	} else if ( sp[3] == JAG_C_COL_TYPE_LINESTRING3D ) {
		return makeJsonLineString3D( "LineString", sp, str );
	} else if ( sp[3] == JAG_C_COL_TYPE_MULTIPOINT ) {
		return makeJsonLineString("MultiPoint", sp, str );
	} else if ( sp[3] == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		return makeJsonLineString3D("MultiPoint", sp, str );
	} else if ( sp[3] == JAG_C_COL_TYPE_POLYGON ) {
		return makeJsonPolygon( "Polygon", sp, str, false );
	} else if ( sp[3] == JAG_C_COL_TYPE_POLYGON3D ) {
		return makeJsonPolygon( "Polygon", sp, str, true );
	} else if ( sp[3] == JAG_C_COL_TYPE_MULTILINESTRING ) {
		return makeJsonPolygon("MultiLineString", sp, str, false );
	} else if ( sp[3] == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		return makeJsonPolygon( "MultiLineString", sp, str, true );
	} else if ( sp[3] == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		return makeJsonMultiPolygon( "MultiPolygon", sp, str, false );
	} else if ( sp[3] == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		return makeJsonMultiPolygon( "MultiPolygon", sp, str, true );
	} else {
		return makeJsonDefault( sp, str) ;
	}
}

/******************************************************************
** GeoJSON supports the following geometry types: 
** Point, LineString, Polygon, MultiPoint, MultiLineString, and MultiPolygon. 
** Geometric objects with additional properties are Feature objects. 
** Sets of features are contained by FeatureCollection objects.
** https://tools.ietf.org/html/rfc7946
*******************************************************************/

// sp: _OJAG_=0=test.lstr.ls=LS guarantee 3 '=' signs
// str: "xmin:ymin:xmax:ymax x:y x:y x:y ..." 
/*********************
    {
       "type": "Feature",
       "bbox": [-10.0, -10.0, 10.0, 10.0],
       "geometry": {
           "type": "LineString",
           "coordinates": [
                   [-10.0, -10.0],
                   [10.0, -10.0],
                   [10.0, 10.0],
                   [-10.0, -10.0]
           ]
       }
       //...
    }

    {
       "type": "Feature",
       "bbox": [-10.0, -10.0, 10.0, 10.0],
       "geometry": {
           "type": "Polygon",
           "coordinates": [
               [
                   [-10.0, -10.0],
                   [10.0, -10.0],
                   [10.0, 10.0],
                   [-10.0, -10.0]
               ]
           ]
       }
       //...
    }
****************/
AbaxDataString JagGeo::makeJsonLineString( const AbaxDataString &title, const JagStrSplit &sp, const char *str )
{
	//prt(("s2980 makeJsonLineString str=[%s]\n", str ));
	const char *p = str;
	while ( *p != ' ' && *p != '\0' ) ++p;
	if ( *p == '\0' ) return "";

	AbaxDataString s;
	rapidjson::StringBuffer bs;
	rapidjson::Writer<rapidjson::StringBuffer> writer(bs);
	writer.StartObject();
	writer.Key("type");
	writer.String("Feature");

	JagStrSplit bsp( AbaxDataString(str, p-str), ':' );
	if ( bsp.length() == 4 ) {
		writer.Key("bbox");
		writer.StartArray();
		writer.Double( jagatof(bsp[0].c_str()) );
		writer.Double( jagatof(bsp[1].c_str()) );
		writer.Double( jagatof(bsp[2].c_str()) );
		writer.Double( jagatof(bsp[3].c_str()) );
		writer.EndArray();
	} else if ( bsp.length() == 6 ) {
		writer.Key("bbox");
		writer.StartArray();
		writer.Double( jagatof(bsp[0].c_str()) );
		writer.Double( jagatof(bsp[1].c_str()) );
		writer.Double( jagatof(bsp[2].c_str()) );
		writer.Double( jagatof(bsp[3].c_str()) );
		writer.Double( jagatof(bsp[4].c_str()) );
		writer.Double( jagatof(bsp[5].c_str()) );
		writer.EndArray();
	}

	while ( isspace(*p) ) ++p; //  "x:y x:y x:y ..."
	char *q = (char*)p;

	writer.Key("geometry");
	writer.StartObject();
	    writer.Key("type");
	    writer.String( title.c_str() );
		writer.Key("coordinates");
		writer.StartArray(); 
		while( *q != '\0' ) {
			//prt(("s2029 q=[%s]\n", q ));
			writer.StartArray(); 
			while (*q != ':' && *q != '\0' ) ++q;
			if ( *q == '\0' ) {
				writer.EndArray(); 
				break;
			}
			//*q = '\0';
			s = AbaxDataString(p, q-p);
			//writer.String( p );
			//writer.String( s.c_str(), s.size() );
			writer.Double( jagatof(s.c_str()) );
			//*q = ':';

			++q;
			//prt(("s2039 q=[%s]\n", q ));
			p = q;
			while (*q != ' ' && *q != '\0' ) ++q;
			if ( *q == '\0' ) {
				//writer.String( p );
				writer.Double( jagatof(p) );
				writer.EndArray(); 
				break;
			}

			//*q = '\0';
			s = AbaxDataString(p, q-p);
			//prt(("s2339 q=[%s]\n", q ));
			//writer.String( p );
			//writer.String( s.c_str(), s.size() );
			writer.Double( jagatof(s.c_str()) );
			writer.EndArray(); 
			//*q = ' ';

			while (*q == ' ' ) ++q;
			p = q;
			//prt(("s1336 q=[%s]\n", q ));
		}
		writer.EndArray(); 
	writer.EndObject();

	writer.Key("properties");
	writer.StartObject();
	    writer.Key("column");
	    writer.String( sp[2].c_str() );
	    writer.Key("srid");
	    writer.String( sp[1].c_str() );
	    writer.Key("dimension");
	    writer.String( "2" );
	writer.EndObject();

	writer.EndObject();

	//prt(("s0301 got result=[%s]\n", (char*)bs.GetString() ));

	return (char*)bs.GetString();
}

AbaxDataString JagGeo::makeJsonLineString3D( const AbaxDataString &title, const JagStrSplit &sp, const char *str )
{
	//prt(("s0823 makeJsonLineString3D str=[%s]\n", str ));

	const char *p = str;
	while ( *p != ' ' && *p != '\0' ) ++p;
	if ( *p == '\0' ) return "";

	AbaxDataString s;
	rapidjson::StringBuffer bs;
	rapidjson::Writer<rapidjson::StringBuffer> writer(bs);
	writer.StartObject();
	writer.Key("type");
	writer.String("Feature");

	JagStrSplit bsp( AbaxDataString(str, p-str), ':' );
	if ( bsp.length() >= 6 ) {
		writer.Key("bbox");
		writer.StartArray();
		writer.Double( jagatof(bsp[0].c_str()) );
		writer.Double( jagatof(bsp[1].c_str()) );
		writer.Double( jagatof(bsp[2].c_str()) );
		writer.Double( jagatof(bsp[3].c_str()) );
		writer.Double( jagatof(bsp[4].c_str()) );
		writer.Double( jagatof(bsp[5].c_str()) );
		writer.EndArray();
	}

	while ( isspace(*p) ) ++p;  // "x:y:z x:y:z x:y:z ..."
	char *q = (char*)p;

	writer.Key("geometry");
	writer.StartObject();
	    writer.Key("type");
	    // writer.String("LineString");
	    writer.String( title.c_str() );
		writer.Key("coordinates");
		writer.StartArray(); 
		while( *q != '\0' ) {
			//prt(("s8102 q=[%s]\n", q ));
			writer.StartArray(); 

			while (*q != ':' && *q != '\0' ) ++q;
			if ( *q == '\0' ) {
				writer.EndArray(); 
				break;
			}
			//*q = '\0';
			s = AbaxDataString(p, q-p);
			//writer.String( p );
			//writer.String( s.c_str(), s.size() );
			writer.Double( jagatof(s.c_str()) );
			//*q = ':';

			++q;
			//prt(("s8103 q=[%s]\n", q ));
			p = q;
			while (*q != ':' && *q != '\0' ) ++q;
			if ( *q == '\0' ) {
				writer.EndArray(); 
				break;
			}
			//*q = '\0';
			s = AbaxDataString(p, q-p);
			//writer.String( p );
			//writer.String( s.c_str(), s.size() );
			writer.Double( jagatof(s.c_str()) );
			//*q = ':';

			++q;
			//prt(("s8104 q=[%s]\n", q ));
			p = q;
			while (*q != ' ' && *q != '\0' ) ++q;
			if ( *q == '\0' ) {
				//writer.String( p );
				writer.Double( jagatof(p) );
				writer.EndArray(); 
				break;
			}

			//*q = '\0';
			s = AbaxDataString(p, q-p);
			// writer.String( p );
			//writer.String( s.c_str(), s.size() );
			writer.Double( jagatof(s.c_str()) );
			writer.EndArray(); 
			//*q = ' ';

			while (*q == ' ' ) ++q;
			//prt(("s8105 q=[%s]\n", q ));
			p = q;
		}
		writer.EndArray(); 
	writer.EndObject();

	writer.Key("properties");
	writer.StartObject();
	    writer.Key("column");
	    writer.String( sp[2].c_str() );
	    writer.Key("srid");
	    writer.String( sp[1].c_str() );
	    writer.Key("dimension");
	    writer.String( "3" );
	writer.EndObject();

	writer.EndObject();

	return (char*)bs.GetString();
}

/********************************************************
{
   "type": "Polygon",
   "coordinates": [
       [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ],
       [ [100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2] ]
   ]
}
********************************************************/
AbaxDataString JagGeo::makeJsonPolygon( const AbaxDataString &title,  const JagStrSplit &sp, const char *str, bool is3D )
{
	//prt(("s7081 makeJsonPolygon str=[%s] is3D=%d\n", str, is3D ));

	const char *p = str;
	while ( *p != ' ' && *p != '\0' ) ++p;
	if ( *p == '\0' ) return "";

	rapidjson::StringBuffer bs;
	rapidjson::Writer<rapidjson::StringBuffer> writer(bs);
	writer.StartObject();
	writer.Key("type");
	writer.String("Feature");

	AbaxDataString bbox(str, p-str);
	//prt(("s5640 bbox=[%s]\n", bbox.c_str() ));
	JagStrSplit bsp( bbox, ':' );
	//prt(("s5732 bsp.len=%d\n", bsp.length() ));
	if ( bsp.length() == 4 ) {
		writer.Key("bbox");
		writer.StartArray();
		writer.Double( jagatof(bsp[0].c_str()) );
		writer.Double( jagatof(bsp[1].c_str()) );
		writer.Double( jagatof(bsp[2].c_str()) );
		writer.Double( jagatof(bsp[3].c_str()) );
		writer.EndArray();
	} else if ( bsp.length() == 6 ) {
		writer.Key("bbox");
		writer.StartArray();
		writer.Double( jagatof(bsp[0].c_str()) );
		writer.Double( jagatof(bsp[1].c_str()) );
		writer.Double( jagatof(bsp[2].c_str()) );
		writer.Double( jagatof(bsp[3].c_str()) );
		writer.Double( jagatof(bsp[4].c_str()) );
		writer.Double( jagatof(bsp[5].c_str()) );
		writer.EndArray();
	}

	while ( isspace(*p) ) ++p; //  "x:y x:y x:y ..."
	char *q = (char*)p;
	AbaxDataString s;
	//int level = 0;

	writer.Key("geometry");
	writer.StartObject();
	    writer.Key("type");
	    //writer.String("Polygon");
	    writer.String( title.c_str() );
		writer.Key("coordinates");
		writer.StartArray(); 
		//++level;
		bool startRing = true;
		while( *q != '\0' ) {
			while (*q == ' ' ) ++q; 
			p = q;
			//prt(("s2029 q=[%s] level=%d p=[%s]\n", q, level, p ));
			if ( startRing ) {
				writer.StartArray(); 
				//++level;
				startRing = false;
			}

			while (*q != ':' && *q != '\0' && *q != '|' ) ++q;
			//prt(("s2132 q=[%s] level=%d\n", q, level ));
			if ( *q == '\0' ) {
				writer.EndArray(); // outeraray
				//--level;
				//prt(("s3362 level=%d break\n", level ));
				break;
			}
			//*q = '\0';
			//s = AbaxDataString(p, q-p);

			if ( *q == '|' ) {
				writer.EndArray(); // outeraray
				startRing = true;
				//--level;
				//prt(("s3462 level=%d continue\n", level ));
				++q;
				p = q;
				continue;
			}

			s = AbaxDataString(p, q-p);

			writer.StartArray(); 
			//++level;
			//prt(("s6301 write xcoord s=[%s]\n", s.c_str() ));
			//writer.String( p );   // x-coord
			//writer.String( s.c_str(), s.size() );   // x-coord
			writer.Double( jagatof(s.c_str()) );
			//*q = save;

			++q;
			p = q;
			//prt(("s2039 q=[%s] p=[%s]\n", q, p ));
			if ( is3D ) {
				while ( *q != ':' && *q != '\0' && *q != '|' ) ++q;
			} else {
				while ( *q != ' ' && *q != '\0' && *q != '|' ) ++q;
			}

			s = AbaxDataString(p, q-p);
			//prt(("s6302 write ycoord s=[%s]\n", s.c_str() ));
			//writer.String( s.c_str(), s.size() );   // y-coord
			writer.Double( jagatof(s.c_str()) );

			if ( is3D && *q != '\0' ) {
				++q;
				p = q;
				//prt(("s2039 q=[%s] p=[%s]\n", q, p ));
				while ( *q != ' ' && *q != '\0' && *q != '|' ) ++q;
				//prt(("s6303 write zcoord s=[%s]\n", s.c_str() ));
				s = AbaxDataString(p, q-p);
				//writer.String( s.c_str(), s.size() );   // z-coord
				writer.Double( jagatof(s.c_str()) );
			}

			writer.EndArray(); // inner raray
			//--level;

			if ( *q == '\0' ) {
				writer.EndArray(); // outeraray
				//--level;
				//prt(("s3162 level=%d break\n", level ));
				break;
			}

			if ( *q == '|' ) {
				writer.EndArray(); // outer raray
				startRing = true;
				//--level;
				//prt(("s1162 level=%d continue p=[%s]\n", level, p ));
				++q;
				p = q;
				continue;
			}

			while (*q == ' ' ) ++q;  // goto next x:y coord
			//prt(("s2339 q=[%s]\n", q ));
			if ( *q == '\0' ) {
				writer.EndArray(); // outeraray
				//--level;
				//prt(("s5862 level=%d break \n", level ));
				break;
			}

			p = q;
		}

		writer.EndArray(); 
		//--level;
		//prt(("s5869 level=%d outside loop \n", level ));
	writer.EndObject();

	writer.Key("properties");
	writer.StartObject();
	    writer.Key("column");
	    writer.String( sp[2].c_str() );
	    writer.Key("srid");
	    writer.String( sp[1].c_str() );
	    writer.Key("dimension");
		if ( is3D ) {
	    	writer.String( "3" );
		} else {
	    	writer.String( "2" );
		}
	writer.EndObject();

	writer.EndObject();

	//prt(("s0303 got result=[%s]\n", (char*)bs.GetString() ));

	return (char*)bs.GetString();
}

/***********************************************************************************
{
   "type": "MultiPolygon",
   "coordinates": [
       [
           [ [102.0, 2.0], [103.0, 2.0], [103.0, 3.0], [102.0, 3.0], [102.0, 2.0] ]
       ],
       [
           [ [100.0, 0.0], [101.0, 0.0], [101.0, 1.0], [100.0, 1.0], [100.0, 0.0] ],
           [ [100.2, 0.2], [100.8, 0.2], [100.8, 0.8], [100.2, 0.8], [100.2, 0.2] ]
       ]
   ]
}
***********************************************************************************/
AbaxDataString JagGeo::makeJsonMultiPolygon( const AbaxDataString &title,  const JagStrSplit &sp, const char *str, bool is3D )
{
	//prt(("s7084 makeJsonMultiPolygon str=[%s] is3D=%d\n", str, is3D ));

	const char *p = str;
	while ( *p != ' ' && *p != '\0' ) ++p;
	if ( *p == '\0' ) return "";

	rapidjson::StringBuffer bs;
	rapidjson::Writer<rapidjson::StringBuffer> writer(bs);
	writer.StartObject();
	writer.Key("type");
	writer.String("Feature");

	AbaxDataString bbox(str, p-str);
	//prt(("s5640 bbox=[%s]\n", bbox.c_str() ));
	JagStrSplit bsp( bbox, ':' );
	//prt(("s5732 bsp.len=%d\n", bsp.length() ));
	if ( bsp.length() == 4 ) {
		writer.Key("bbox");
		writer.StartArray();
		writer.Double( jagatof(bsp[0].c_str()) );
		writer.Double( jagatof(bsp[1].c_str()) );
		writer.Double( jagatof(bsp[2].c_str()) );
		writer.Double( jagatof(bsp[3].c_str()) );
		writer.EndArray();
	} else if ( bsp.length() == 6 ) {
		writer.Key("bbox");
		writer.StartArray();
		writer.Double( jagatof(bsp[0].c_str()) );
		writer.Double( jagatof(bsp[1].c_str()) );
		writer.Double( jagatof(bsp[2].c_str()) );
		writer.Double( jagatof(bsp[3].c_str()) );
		writer.Double( jagatof(bsp[4].c_str()) );
		writer.Double( jagatof(bsp[5].c_str()) );
		writer.EndArray();
	}

	while ( isspace(*p) ) ++p; //  "x:y x:y x:y ..."
	char *q = (char*)p;
	AbaxDataString s;
	//int level = 0;

	writer.Key("geometry");
	writer.StartObject();
	    writer.Key("type");
	    writer.String( title.c_str() );
		writer.Key("coordinates");
		writer.StartArray(); 
		//++level;
		bool startPolygon = true;
		bool startRing = true;
		while( *q != '\0' ) {
			while (*q == ' ' ) ++q; 
			p = q;
			//prt(("s2029 q=[%s] level=%d p=[%s]\n", q, level, p ));

			if ( startPolygon ) {
				writer.StartArray(); 
				//++level;
				startPolygon = false;
			}

			if ( startRing ) {
				writer.StartArray(); 
				//++level;
				startRing = false;
			}

			while (*q != ':' && *q != '\0' && *q != '|' && *q != '!' ) ++q;
			//prt(("s2132 q=[%s] level=%d\n", q, level ));
			if ( *q == '\0' ) {
				writer.EndArray(); // outeraray
				writer.EndArray(); // outeraray
				//--level;
				//--level;
				//prt(("s3362 level=%d break\n", level ));
				break;
			}

			if ( *q == '|' ) {
				writer.EndArray(); // outeraray
				startRing = true;
				//--level;
				//prt(("s3462 level=%d continue\n", level ));
				++q;
				p = q;
				continue;
			}

			if ( *q == '!' ) {
				writer.EndArray(); // outeraray
				writer.EndArray(); // outeraray
				startPolygon = true;
				startRing = true;
				//--level;
				//--level;
				//prt(("s3462 level=%d continue\n", level ));
				++q;
				p = q;
				continue;
			}

			s = AbaxDataString(p, q-p);

			writer.StartArray(); 
			//++level;
			//prt(("s6301 write xcoord p=[%s]\n", p ));
			//writer.String( s.c_str(), s.size() );   // x-coord
			writer.Double( jagatof(s.c_str()) );

			++q;
			p = q;
			//prt(("s2039 q=[%s] p=[%s]\n", q, p ));
			if ( is3D ) {
				while ( *q != ':' && *q != '\0' && *q != '|' && *q != '!' ) ++q;
			} else {
				while ( *q != ' ' && *q != '\0' && *q != '|' && *q != '!' ) ++q;
			}

			s = AbaxDataString(p, q-p);
			//writer.String( s.c_str(), s.size() );   // y-coord
			writer.Double( jagatof(s.c_str()) );

			if ( is3D && *q != '\0' ) {
				++q;
				p = q;
				//prt(("s2039 q=[%s] p=[%s]\n", q, p ));
				while ( *q != ' ' && *q != '\0' && *q != '|' ) ++q;
				s = AbaxDataString(p, q-p);
				//writer.String( s.c_str(), s.size() );   // z-coord
				writer.Double( jagatof(s.c_str()) );
			}

			writer.EndArray(); // inner raray
			//--level;

			if ( *q == '\0' ) {
				writer.EndArray(); // outeraray
				writer.EndArray(); // outeraray
				//--level;
				//--level;
				//prt(("s3162 level=%d break\n", level ));
				break;
			}

			if ( *q == '|' ) {
				writer.EndArray(); // outer raray
				startRing = true;
				//--level;
				//prt(("s1162 level=%d continue p=[%s]\n", level, p ));
				++q;
				p = q;
				continue;
			}

			if ( *q == '!' ) {
				writer.EndArray(); // outer raray
				writer.EndArray(); // outer raray
				startPolygon = true;
				startRing = true;
				//--level;
				//--level;
				//prt(("s1162 level=%d continue p=[%s]\n", level, p ));
				++q;
				p = q;
				continue;
			}

			while (*q == ' ' ) ++q;  // goto next x:y coord
			//prt(("s2339 q=[%s]\n", q ));
			if ( *q == '\0' ) {
				writer.EndArray(); // outeraray
				writer.EndArray(); // outeraray
				//--level;
				//--level;
				//prt(("s5862 level=%d break \n", level ));
				break;
			}

			p = q;
		}

		writer.EndArray(); 
		//--level;
		//prt(("s5869 level=%d outside loop \n", level ));
	writer.EndObject();

	writer.Key("properties");
	writer.StartObject();
	    writer.Key("column");
	    writer.String( sp[2].c_str() );
	    writer.Key("srid");
	    writer.String( sp[1].c_str() );
	    writer.Key("dimension");
		if ( is3D ) {
	    	writer.String( "3" );
		} else {
	    	writer.String( "2" );
		}
	writer.EndObject();

	writer.EndObject();

	//prt(("s0303 got result=[%s]\n", (char*)bs.GetString() ));

	return (char*)bs.GetString();
}

AbaxDataString JagGeo::makeJsonDefault( const JagStrSplit &sp, const char *str )
{
	return "";
}

// 2D case
bool JagGeo::pointWithinPolygon( double x, double y, const JagLineString3D &linestr )
{
	if ( linestr.size() < 4 ) {
		//prt(("s2760 pointWithinPolygon linestr.size=%d\n", linestr.size() ));
		return false;
	}
	JagPoint2D point(x,y);
	JagPoint2D p1( linestr.point[0].x, linestr.point[0].y );

	// linestr.point[i].x.y.z
	int cnt = 0;
	for ( int i = 1; i <= linestr.size()-3; ++i ) {
		JagPoint2D p2( linestr.point[i].x, linestr.point[i].y );
		JagPoint2D p3( linestr.point[i+1].x, linestr.point[i+1].y );
		if ( pointWithinTriangle( point, p1, p2, p3, false, false ) ) {
			++cnt;
		}
	}

	if ( ( cnt%2 ) == 1 ) {
		// odd number
		//prt(("3039 cnt=%d odd true\n" ));
		return true;
	} else {
		//prt(("3034 cnt=%d even false\n" ));
		return false;
	}

}

void JagGeo::getPolygonBound( const AbaxDataString &mk, const JagStrSplit &sp, 
							  double &bbx, double &bby, double &rx, double &ry )
{
    if ( mk == JAG_OJAG ) {
        boundingBoxRegion(sp[0], bbx, bby, rx, ry );
    } else {
		double xmin, ymin, xmax, ymax;
		xmin = ymin = LONG_MAX; xmax = ymax = LONG_MIN;
		/***
        char *p = (char*)sp.c_str();
        while ( *p == ' ' ) ++p;  // goto first non empty char
        while ( *p != ' ' && *p != '\0' ) ++p; // go over first non-empty tokens
        if ( *p == '\0' ) return;
        while ( *p == ' ' ) ++p;  // goto first non empty char
		***/
		char *p = secondTokenStart( sp.c_str() );
		if ( ! p ) return;
        JagParser::getPolygonMinMax( p, xmin, ymin, xmax, ymax );
        bbx = ( xmin+xmax)/2.0; bby = ( ymin+ymax)/2.0;
        rx = (xmax-xmin)/2.0; ry = (ymax-ymin)/2.0;
    }
}

void JagGeo::getLineStringBound( const AbaxDataString &mk, const JagStrSplit &sp, 
							  double &bbx, double &bby, double &rx, double &ry )
{
    if ( mk == JAG_OJAG ) {
        boundingBoxRegion(sp[0], bbx, bby, rx, ry );
    } else {
		double xmin, ymin, xmax, ymax;
		xmin = ymin = LONG_MAX; xmax = ymax = LONG_MIN;
		/***
        char *p = (char*)sp.c_str();
        while ( *p == ' ' ) ++p;  // goto first non empty char
        while ( *p != ' ' && *p != '\0' ) ++p; // go over first non-empty tokens
        if ( *p == '\0' ) return;
        while ( *p == ' ' ) ++p;  // goto first non empty char
		***/
		char *p = secondTokenStart( sp.c_str() );
		prt(("s2074 getLineStringBound p=[%s]\n", p ));
		if ( ! p ) return;
        JagParser::getLineStringMinMax( p, xmin, ymin, xmax, ymax );
		prt(("s2227 xmin=%.2f ymin=%.2f xmax=%.2f ymax=%.2f\n",  xmin, ymin, xmax, ymax ));
        bbx = ( xmin+xmax)/2.0; bby = ( ymin+ymax)/2.0;
        rx = (xmax-xmin)/2.0; ry = (ymax-ymin)/2.0;
    }
}

bool JagGeo::pointWithinPolygon( double x, double y, const JagPolygon &pgon )
{
	if ( pgon.size() <1 ) {
		prt(("s7080 pointWithinPolygon false\n" ));
		return false;
	}

	if ( ! pointWithinPolygon( x, y, pgon.linestr[0] ) ) {
		prt(("s7082 pointWithinPolygon false\n" ));
		return false;
	}

	for ( int i=1; i < pgon.size(); ++i ) {
		if ( pointWithinPolygon( x, y, pgon.linestr[i] ) ) {
			prt(("s7084 i=%d pointWithinPolygon false\n" ));
			return false;  // in holes
		}
	}

	return true;
}


double JagGeo::dotProduct( double x1, double y1, double x2, double y2 )
{
	return ( x1*x2 + y1*y2 );
}
double JagGeo::dotProduct( double x1, double y1, double z1, double x2, double y2, double z2 )
{
	return ( x1*x2 + y1*y2 + z1*z2 );
}

void JagGeo::crossProduct( double x1, double y1, double z1, double x2, double y2, double z2, 
						   double &x, double &y, double &z )
{
	x = y1*z2 - z1*y2;
	y = z1*x2 - x1*z2;
	z = x1*y2 - y1*x2;
}

// return 0: not insersect
// return 1: intersection falls inside the triagle
// return 2: intersection does not fall in the triangle, but falls on the plane. atPoint returns coord
int JagGeo::line3DIntersectTriangle3D( const JagLine3D& line3d, 
								       const JagPoint3D &p0, const JagPoint3D &p1, const JagPoint3D &p2,
	                                   JagPoint3D &atPoint )
{
	// https://en.wikipedia.org/wiki/Line–plane_intersection
	JagPoint3D negab;
	negab.x = line3d.x1 - line3d.x2; 
	negab.y = line3d.y1 - line3d.y2; 
	negab.z = line3d.z1 - line3d.z2; 

	JagPoint3D p01;
	p01.x = p1.x - p0.x; p01.y = p1.y - p0.y; p01.z = p1.z - p0.z;

	JagPoint3D p02;
	p02.x = p2.x - p0.x; p02.y = p2.y - p0.y; p02.z = p2.z - p0.z;
	
	JagPoint3D LA0;
	LA0.x = line3d.x1 - p0.x; LA0.y = line3d.y1 - p0.y; LA0.z = line3d.z1 - p0.z;


	double crossx, crossy, crossz;
	crossProduct( p01.x, p01.y, p01.z, p02.x, p02.y, p02.z, crossx, crossy, crossz );
	double dotLow = dotProduct( negab.x, negab.y, negab.z, crossx, crossy, crossz );
	// t = dotUp / dotLow;
	if ( jagEQ( dotLow, 0.0 ) ) {
		// either in plane or parallel
		if ( point3DWithinTriangle3D( line3d.x1, line3d.y1, line3d.z1, p0, p1, p2 ) ) return 1;
		if ( point3DWithinTriangle3D( line3d.x2, line3d.y2, line3d.z2, p0, p1, p2 ) ) return 1;
		return 0;
	}

	double dotUp = dotProduct( crossx, crossy, crossz, LA0.x, LA0.y, LA0.z );
	double t = dotUp/dotLow;
	if ( t < 0.0 || t > 1.0 ) return 0; // intersection is out of line segment  line3d

	crossProduct( p02.x, p02.y, p02.z, negab.x, negab.y, negab.z, crossx, crossy, crossz );
	dotUp = dotProduct( crossx, crossy, crossz, LA0.x, LA0.y, LA0.z );
	double u = dotUp/dotLow;

	crossProduct( negab.x, negab.y, negab.z, p01.x, p01.y, p01.z, crossx, crossy, crossz );
	dotUp = dotProduct( crossx, crossy, crossz, LA0.x, LA0.y, LA0.z );
	double v = dotUp/dotLow;
	if ( jagLE( u+v, 1.0) ) return 1;

	// la + lab * t
	atPoint.x =  line3d.x1 - negab.x * t;
	atPoint.y =  line3d.y1 - negab.y * t;
	atPoint.z =  line3d.z1 - negab.z * t;
	return 2;
}

// inside the plane ?
bool JagGeo::point3DWithinTriangle3D( double x, double y, double z, const JagPoint3D &p1, const JagPoint3D &p2, const JagPoint3D &p3 )
{
	JagPoint3D pt(x,y,z);
	double u, v, w;
	int rc = getBarycentric(pt, p1, p2, p3, u, v, w );
	if ( rc < 0 ) return false;

	if ( u < 0.0 || u > 1.0 ) return false;
	if ( v < 0.0 || v > 1.0 ) return false;
	if ( w < 0.0 || w > 1.0 ) return false;
	return true;
}

double JagGeo::distancePoint3DToTriangle3D( double x, double y, double z,
											double x1, double y1, double z1, double x2, double y2, double z2,
											double x3, double y3, double z3 )
{
	double A, B, C, D;
	triangle3DABCD( x1, y1, z1, x2, y2, z2, x3, y3, z3, A, B, C, D );
	return distancePoint3DToPlane(x,y,z, A, B, C, D );
}

/***
void JagGeo::Barycentric(Point p, Point a, Point b, Point c, float &u, float &v, float &w)
{
    Vector v0 = b - a, v1 = c - a, v2 = p - a;
    float d00 = Dot(v0, v0);
    float d01 = Dot(v0, v1);
    float d11 = Dot(v1, v1);
    float d20 = Dot(v2, v0);
    float d21 = Dot(v2, v1);
    float denom = d00 * d11 - d01 * d01;
    v = (d11 * d20 - d01 * d21) / denom;
    w = (d00 * d21 - d01 * d20) / denom;
    u = 1.0 - v - w;
}
***/
// -1 error  0 ok
int JagGeo::getBarycentric( const JagPoint3D &p, const JagPoint3D &a, const JagPoint3D &b, const JagPoint3D &c, 
						  double &u, double &v, double &w)
{
    //Vector v0 = b - a, v1 = c - a, v2 = p - a;
	JagPoint3D v0, v1, v2;
	minusVector(b, a, v0);
	minusVector(c, a, v1);
	minusVector(p, a, v2);

    double d00 = dotProduct(v0, v0);
    double d01 = dotProduct(v0, v1);
    double d11 = dotProduct(v1, v1);
    double d20 = dotProduct(v2, v0);
    double d21 = dotProduct(v2, v1);
    double denom = d00 * d11 - d01 * d01;
	if ( jagEQ(denom, 0.0) ) return -1;

    v = (d11 * d20 - d01 * d21) / denom;
    w = (d00 * d21 - d01 * d20) / denom;
    u = 1.0 - v - w;
	return 0;
}

void JagGeo::minusVector( const JagPoint3D &v1, const JagPoint3D &v2, JagPoint3D &pt )
{
	pt.x = v1.x - v2.x; pt.y = v1.y - v2.y; pt.z = v1.z - v2.z;
}

double JagGeo::dotProduct( const JagPoint3D &p1, const JagPoint3D &p2 )
{
	return ( p1.x*p2.x + p1.y*p2.y + p1.z*p2.z );
}

double JagGeo::dotProduct( const JagPoint2D &p1, const JagPoint2D &p2 )
{
	return ( p1.x*p2.x + p1.y*p2.y );
}


bool JagGeo::distance( const AbaxFixString &lstr, const AbaxFixString &rstr, const AbaxDataString &arg, double &dist )
{
	prt(("s3083 JagGeo::distance lstr=[%s] rstr=[%s] arg=[%s]\n", lstr.c_str(), rstr.c_str(), arg.c_str() ));

	JagStrSplit sp1( lstr.c_str(), ' ', true );
	if ( sp1.length() < 1 ) return 0;
	JagStrSplit sp2( rstr.c_str(), ' ', true );
	if ( sp2.length() < 1 ) return 0;

	JagStrSplit co1( sp1[0], '=' );
	if ( co1.length() < 4 ) return 0;
	JagStrSplit co2( sp2[0], '=' );
	if ( co2.length() < 4 ) return 0;
	AbaxDataString mark1 = co1[0];
	AbaxDataString mark2 = co2[0];

	AbaxDataString colType1 = co1[3];
	AbaxDataString colType2 = co2[3];
	int dim1 = JagGeo::getDimension( colType1 );
	int dim2 = JagGeo::getDimension( colType2 );
	if ( dim1 != dim2 ) {
		return 0;
	}

	int srid1 = jagatoi( co1[1].c_str() );
	int srid2 = jagatoi( co2[1].c_str() );
	int srid;
	if ( 0 == srid1 ) {
		srid = srid2;
	} else if ( 0 == srid2 ) {
		srid = srid1;
	} else if ( srid2 != srid1 ) {
		return 0;
	}

	#if 0
		prt(("s3220 srid=%d dim1=%d\n", srid, dim1 ));
		if ( 2 == dim1 ) {
			// 2D distance
    		double fx = jagatof( sp1[1].c_str());
    		double fy = jagatof( sp1[2].c_str());
    		double gx = jagatof( sp2[1].c_str());
    		double gy = jagatof( sp2[2].c_str());
    		dist  = JagGeo::distance( fx, fy, gx, gy, srid );
			//prt(("s5003 distance 2=minargs fx=%f fy=%f gx=%f gy=%f res=%f\n", fx, fy, gx, gy, r ));
		} else if ( 3 == dim1 ) {
			// 3D distanc)
    		double fx = jagatof( sp1[1].c_str());
    		double fy = jagatof( sp1[2].c_str());
    		double fz = jagatof( sp1[3].c_str());

    		double gx = jagatof( sp2[1].c_str());
    		double gy = jagatof( sp2[2].c_str());
    		double gz = jagatof( sp2[3].c_str());
    		dist = JagGeo::distance( fx, fy, fz, gx, gy, gz, srid );
			/***
			prt(("s5004 distance 3=minargs colType=[%s] fx=%f fy=%f fz=%f   gx=%f gy=%f gz=%f res=%f\n",
					colType.c_str(), fx, fy, fz, gx, gy, gz, r ));
			***/
		} else {
			// prt(("s8383 minargs=%d\n", minargs ));
    		dist = 0;
		}
	#else

	sp1.shift();
	sp2.shift();
	prt(("s4872 colType1=[%s]\n", colType1.c_str() ));

	if ( colType1 == JAG_C_COL_TYPE_POINT ) {
		return doPointDistance( mark1, sp1, mark2, colType2, sp2, srid, arg, dist );
	} else if ( colType1 == JAG_C_COL_TYPE_POINT3D ) {
		return doPoint3DDistance( mark1, sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE ) {
		return doCircleDistance( mark1,  sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_CIRCLE3D ) {
		return doCircle3DDistance( mark1,  sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_SPHERE ) {
		return doSphereDistance( mark1,  sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE ) {
		return doSquareDistance( mark1,  sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_SQUARE3D ) {
		return doSquare3DDistance( mark1,  sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_CUBE ) {
		return doCubeDistance( mark1,  sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE ) {
		return doRectangleDistance( mark1,  sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_RECTANGLE3D ) {
		return doRectangle3DDistance( mark1,  sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_BOX ) {
		return doBoxDistance( mark1,  sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE ) {
		return doTriangleDistance( mark1,  sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_TRIANGLE3D ) {
		return doTriangle3DDistance( mark1,  sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_CYLINDER ) {
		return doCylinderDistance( mark1,  sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_CONE ) {
		return doConeDistance( mark1,  sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSE ) {
		return doEllipseDistance( mark1,  sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_ELLIPSOID ) {
		return doEllipsoidDistance( mark1,  sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_LINE ) {
		return doLineDistance( mark1,  sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_LINE3D ) {
		return doLine3DDistance( mark1,  sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING ) {
		return doLineStringDistance( mark1,   sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return doLineString3DDistance( mark1,   sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON ) {
		return doPolygonDistance( mark1,   sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_POLYGON3D ) {
		return doPolygon3DDistance( mark1,   sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT ) {
		return doLineStringDistance( mark1,   sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		return doLineString3DDistance( mark1,   sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING ) {
		return doPolygonDistance( mark1,   sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTILINESTRING3D ) {
		return doPolygon3DDistance( mark1,   sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON ) {
		return doMultiPolygonDistance( mark1,   sp1, mark2, colType2, sp2, srid,  arg, dist);
	} else if ( colType1 == JAG_C_COL_TYPE_MULTIPOLYGON3D ) {
		return doMultiPolygon3DDistance( mark1,   sp1, mark2, colType2, sp2, srid,  arg, dist);
	} 
	#endif

	return true;
}


		
/////////////////////////// distance ///////////////////////////////////////////////////////		
bool JagGeo::doPointDistance(const AbaxDataString& mk1, const JagStrSplit& sp1, const AbaxDataString& mk2, const AbaxDataString& colType2,
										 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	if ( colType2 == JAG_C_COL_TYPE_POINT ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		return pointDistancePoint( srid, px0, py0, x0, y0, arg, dist);
	} else if ( colType2 == JAG_C_COL_TYPE_LINE ) {
		double x1 = jagatof( sp2[0].c_str() ); 
		double y1 = jagatof( sp2[1].c_str() ); 
		double x2 = jagatof( sp2[2].c_str() ); 
		double y2 = jagatof( sp2[3].c_str() ); 
		return pointDistanceLine( srid, px0, py0, x1, y1, x2, y2, arg, dist);
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING || colType2 == JAG_C_COL_TYPE_MULTIPOINT ) {
		return pointDistanceLineString( srid, px0, py0, mk2, sp2, arg, dist);
	} else if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		return pointDistanceTriangle( srid, jagatof( sp1[0].c_str()), jagatof( sp1[1].c_str()), 
									  jagatof( sp2[0].c_str() ), jagatof( sp2[1].c_str() ), 
									  jagatof( sp2[2].c_str() ), jagatof( sp2[3].c_str() ), 
									  jagatof( sp2[4].c_str() ), jagatof( sp2[5].c_str() ),
									  arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3);
		return pointDistanceSquare( srid, px0, py0, x0, y0, r, nx, arg, dist);
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double w = jagatof( sp2[2].c_str() ); 
		double h = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		return pointDistanceRectangle( srid, px0, py0, x0, y0, w,h, nx, arg, dist);
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() );
		return pointDistanceCircle( srid, px0, py0, x, y, r, arg, dist);
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double w = jagatof( sp2[2].c_str() );
		double h = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return pointDistanceEllipse( srid, px0, py0, x0, y0, w, h, nx, arg, dist);
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return pointDistancePolygon( srid, px0, py0, mk2, sp2, arg, dist);
	}
	return false;
}

bool JagGeo::doPoint3DDistance( const AbaxDataString& mk1, const JagStrSplit& sp1, const AbaxDataString& mk2, 
							    const AbaxDataString& colType2,
							    const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	prt(("s7730 mk1=[%s] mk2=[%s] sp1:sp2:\n", mk1.c_str(), mk2.c_str() ));
	//sp1.print();
	//sp2.print();

	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	if ( colType2 == JAG_C_COL_TYPE_POINT3D ) {
		double x1 = jagatof( sp2[0].c_str() ); 
		double y1 = jagatof( sp2[1].c_str() ); 
		double z1 = jagatof( sp2[2].c_str() ); 
		return point3DDistancePoint3D( srid, px0, py0, pz0, x1, y1, z1, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_LINE3D ) {
		double x1 = jagatof( sp2[0].c_str() ); 
		double y1 = jagatof( sp2[1].c_str() ); 
		double z1 = jagatof( sp2[2].c_str() ); 
		double x2 = jagatof( sp2[3].c_str() ); 
		double y2 = jagatof( sp2[4].c_str() ); 
		double z2 = jagatof( sp2[5].c_str() ); 
		return point3DDistanceLine3D( srid, px0, py0, pz0, x1, y1, z1, x2, y2, z2, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING3D || colType2 == JAG_C_COL_TYPE_MULTIPOINT3D ) {
		return point3DDistanceLineString3D( srid, px0, py0, pz0, mk2, sp2, arg, dist );
	} else if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return point3DDistanceBox( srid, px0, py0, pz0, x0, y0, z0, r,r,r, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double a = jagatof( sp2[3].c_str() ); 
		double b = jagatof( sp2[4].c_str() ); 
		double c = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return point3DDistanceBox( srid, px0, py0, pz0, x0, y0, z0, a,b,c, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return point3DDistanceSphere( srid, px0,py0,pz0, x, y, z, r, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double a = jagatof( sp2[3].c_str() );
		double b = jagatof( sp2[4].c_str() );
		double c = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return point3DDistanceEllipsoid( srid, px0, py0, pz0, x0, y0, z0, a,b,c, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return point3DDistanceCone( srid, px0,py0,pz0, x, y, z, r, h, nx, ny, arg, dist );
	}
	return false;
}

bool JagGeo::doCircleDistance(const AbaxDataString& mk1,  const JagStrSplit& sp1, const AbaxDataString& mk2, const AbaxDataString& colType2,
										 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pr0 = jagatof( sp1[2].c_str() ); 

	if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() );
		return circleDistanceCircle( srid, px0,py0,pr0, x,y,r, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() );
		double nx = safeget( sp2, 3);
		return circleDistanceSquare( srid, px0,py0,pr0, x,y,r, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double w = jagatof( sp2[2].c_str() );
		double h = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4);
		return circleDistanceRectangle( srid, px0, py0, pr0, x0, y0, w, h, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		double x1 = jagatof( sp2[0].c_str() ); 
		double y1 = jagatof( sp2[1].c_str() ); 
		double x2 = jagatof( sp2[2].c_str() ); 
		double y2 = jagatof( sp2[3].c_str() ); 
		double x3 = jagatof( sp2[4].c_str() ); 
		double y3 = jagatof( sp2[5].c_str() ); 
		return circleDistanceTriangle( srid, px0, py0, pr0, x1, y1, x2, y2, x3, y3, arg, dist  );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4);
		return circleDistanceEllipse( srid, px0, py0, pr0, x0, y0, a, b, nx, arg, dist  );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return circleDistancePolygon( srid, px0, py0, pr0, mk2, sp2, arg, dist );
	}
	return false;
}

bool JagGeo::doCircle3DDistance(const AbaxDataString& mk1,  const JagStrSplit& sp1, const AbaxDataString& mk2, 
								const AbaxDataString& colType2, const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double pr0 = jagatof( sp1[3].c_str() ); 

	double nx0 = 0.0;
	double ny0 = 0.0;
	if ( sp1.length() >= 5 ) { nx0 = jagatof( sp1[4].c_str() ); }
	if ( sp1.length() >= 6 ) { ny0 = jagatof( sp1[5].c_str() ); }

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return circle3DDistanceCube( srid, px0, py0, pz0, pr0, nx0, ny0, x0, y0, z0, r, nx, ny, arg, dist );
	} else if (  colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double a = jagatof( sp2[3].c_str() ); 
		double b = jagatof( sp2[4].c_str() ); 
		double c = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return circle3DDistanceBox( srid, px0, py0, pz0, pr0, nx0, ny0, x0, y0, z0, a,b,c, nx, ny, arg, dist );
	} else if (  colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return circle3DDistanceSphere( srid, px0, py0, pz0, pr0, nx0, ny0, x, y, z, r, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return circle3DDistanceEllipsoid( srid, px0, py0, pz0, pr0, nx0, ny0, x0, y0, z0, w,d,h,  nx,ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5);
		double ny = safeget( sp2, 6);
		return circle3DDistanceCone( srid, px0,py0,pz0,pr0,nx0,ny0, x, y, z, r, h, nx, ny, arg, dist );
	}

	return false;
}

double JagGeo::doSphereArea( int srid1, const JagStrSplit& sp1 )
{
	double r = jagatof( sp1[2].c_str() ); 
	return r * r * 4.0 * JAG_PI;
}

bool JagGeo::doSphereDistance(const AbaxDataString& mk1,  const JagStrSplit& sp1, const AbaxDataString& mk2, const AbaxDataString& colType2,
										 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double pr0 = jagatof( sp1[2].c_str() ); 
	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4);
		double ny = safeget( sp2, 5);
		return sphereDistanceCube( srid, px0, py0, pz0, pr0, x0, y0, z0, r, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 

		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return sphereDistanceBox( srid, px0, py0, pz0, pr0, x0, y0, z0, w,d,h, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return sphereDistanceSphere( srid, px0, py0, pz0, pr0, x, y, z, r, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return sphereDistanceEllipsoid( srid, px0, py0, pz0, pr0, x0, y0, z0, w,d,h, nx,ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5);
		double ny = safeget( sp2, 6);
		return sphereDistanceCone( srid, px0, py0, pz0, pr0,    x0, y0, z0, r,h, nx,ny, arg, dist );
	}
	return false;
}

bool JagGeo::doSquareDistance(const AbaxDataString& mk1,  const JagStrSplit& sp1, const AbaxDataString& mk2, 
							const AbaxDataString& colType2, const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pr0 = jagatof( sp1[2].c_str() ); 
	double nx0 = safeget( sp1, 3 );

	// like point Distance
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		// return squareDistanceTriangle( srid, px0, py0, pr0, nx0, x1, y1, x2, y2, x3, y3, arg, dist );
		return rectangleDistanceTriangle( srid, px0, py0, pr0,pr0, nx0, x1, y1, x2, y2, x3, y3, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double nx = safeget(sp2, 3 );
		return rectangleDistanceSquare( srid, px0, py0, pr0,pr0, nx0, x0, y0, a, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		double nx = safeget(sp2, 4 );
		return rectangleDistanceRectangle( srid, px0, py0, pr0,pr0, nx0, x0, y0, a, b, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		double nx = safeget(sp2, 3);
		return rectangleDistanceCircle( srid, px0, py0, pr0,pr0, nx0, x0, y0, r, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		double nx = safeget(sp2, 4);
		return rectangleDistanceEllipse( srid, px0, py0, pr0,pr0, nx0, x0, y0, a, b, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return rectangleDistancePolygon( srid, px0, py0, pr0,pr0,nx0, mk2, sp2, arg, dist );
	}
	return false;
}

bool JagGeo::doSquare3DDistance(const AbaxDataString& mk1,  const JagStrSplit& sp1, const AbaxDataString& mk2, 
							const AbaxDataString& colType2, const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double pr0 = jagatof( sp1[3].c_str() ); 
	double nx0 = safeget( sp1, 4);
	double ny0 = safeget( sp1, 5);

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		// return square3DDistanceCube( srid, px0, py0, pz0, pr0, nx0, ny0, x0, y0, z0, r, nx, ny, arg, dist );
		return rectangle3DDistanceCube( srid, px0, py0, pz0, pr0,pr0, nx0, ny0, x0, y0, z0, r, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		// return square3DDistanceBox( srid, px0, py0, pz0, pr0,  nx0, ny0,x0, y0, z0, w,d,h, nx, ny, arg, dist );
		return rectangle3DDistanceBox( srid, px0, py0, pz0, pr0,pr0, nx0, ny0,x0, y0, z0, w,d,h, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		// return square3DDistanceSphere( srid, px0, py0, pz0, pr0, nx0, ny0, x, y, z, r, arg, dist );
		return rectangle3DDistanceSphere( srid, px0, py0, pz0, pr0,pr0, nx0, ny0, x, y, z, r, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		// return square3DDistanceEllipsoid( srid, px0, py0, pz0, pr0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, arg, dist );
		return rectangle3DDistanceEllipsoid( srid, px0, py0, pz0, pr0,pr0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		// return square3DDistanceCone( srid, px0, py0, pz0, pr0, nx0, ny0, x0, y0, z0, r,h, nx,ny, arg, dist );
		return rectangle3DDistanceCone( srid, px0, py0, pz0, pr0,pr0, nx0, ny0, x0, y0, z0, r,h, nx,ny, arg, dist );
	}
	return false;

}

double JagGeo::doCubeArea( int srid1, const JagStrSplit& sp1 )
{
	double r = jagatof( sp1[3].c_str() ); 
	return (r*r*24.0);  // 2r*2r*6
}

bool JagGeo::doCubeDistance(const AbaxDataString& mk1,  const JagStrSplit& sp1, const AbaxDataString& mk2, const AbaxDataString& colType2,
										 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double pr0 = jagatof( sp1[3].c_str() ); 
	double nx0 = safeget(sp1, 4);
	double ny0 = safeget(sp1, 5);

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return boxDistanceCube( srid, px0, py0, pz0, pr0,pr0,pr0, nx0, ny0, x0, y0, z0, r, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return boxDistanceBox( srid, px0, py0, pz0, pr0,pr0,pr0, nx0, ny0, x0, y0, z0, w,d,h, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return boxDistanceSphere( srid, px0, py0, pz0, pr0,pr0,pr0, nx0, ny0, x, y, z, r, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return boxDistanceEllipsoid( srid, px0, py0, pz0, pr0,pr0,pr0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return boxDistanceCone( srid, px0, py0, pz0, pr0,pr0,pr0, nx0, ny0, x0, y0, z0, r, h, nx,ny, arg, dist );
	}
	return false;
}

double JagGeo::doRectangleArea( int srid1, const JagStrSplit& sp1 )
{
	double a = jagatof( sp1[2].c_str() ); 
	double b = jagatof( sp1[3].c_str() ); 
	return a*b* 4.0;
}
double JagGeo::doRectangle3DArea( int srid1, const JagStrSplit& sp1 )
{
	double a = jagatof( sp1[3].c_str() ); 
	double b = jagatof( sp1[4].c_str() ); 
	return a*b* 4.0;
}

bool JagGeo::doRectangleDistance(const AbaxDataString& mk1,  const JagStrSplit& sp1, const AbaxDataString& mk2, 
						const AbaxDataString& colType2, const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double a0 = jagatof( sp1[2].c_str() ); 
	double b0 = jagatof( sp1[3].c_str() ); 
	double nx0 = safeget( sp1, 4 );

	// like point Distance
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return rectangleDistanceTriangle( srid, px0, py0, a0, b0, nx0, x1, y1, x2, y2, x3, y3, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3 );
		return rectangleDistanceSquare( srid, px0, py0, a0, b0, nx0, x0, y0, a, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		return rectangleDistanceRectangle( srid, px0, py0, a0, b0, nx0, x0, y0, a, b, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3 );
		return rectangleDistanceCircle( srid, px0, py0, a0, b0, nx0, x0, y0, r, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4 );
		return rectangleDistanceEllipse( srid, px0, py0, a0, b0, nx0, x0, y0, a, b, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return rectangleDistancePolygon( srid, px0,py0,a0,b0,nx0, mk2, sp2, arg, dist );
	}
	return false;
}

bool JagGeo::doRectangle3DDistance(const AbaxDataString& mk1,  const JagStrSplit& sp1, const AbaxDataString& mk2, 
						const AbaxDataString& colType2, const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double a0 = jagatof( sp1[3].c_str() ); 
	double b0 = jagatof( sp1[4].c_str() ); 
	double nx0 = safeget( sp1, 5 );
	double ny0 = safeget( sp1, 6 );

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4);
		double ny = safeget( sp2, 5);
		return rectangle3DDistanceCube( srid, px0, py0, pz0, a0, b0, nx0, ny0, x0, y0, z0, r, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return rectangle3DDistanceBox( srid, px0, py0, pz0, a0, b0, nx0, ny0, x0, y0, z0, w,d,h, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return rectangle3DDistanceSphere( srid, px0, py0, pz0, a0, b0, nx0, ny0, x, y, z, r, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return rectangle3DDistanceEllipsoid( srid, px0, py0, pz0, a0, b0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5);
		double ny = safeget( sp2, 6);
		return rectangle3DDistanceCone( srid, px0, py0, pz0, a0, b0, nx0, ny0, x0, y0, z0, r,h, nx,ny, arg, dist );
	}
	return false;
}

double JagGeo::doBoxArea( int srid1, const JagStrSplit& sp1 )
{
	double a = jagatof( sp1[3].c_str() ); 
	double b = jagatof( sp1[4].c_str() ); 
	double c = jagatof( sp1[5].c_str() ); 
	return  (a*b + b*c + c*a ) * 8.0;
	// ( 2a*2b + 2b*2c + 2a*2c )*2 
}

bool JagGeo::doBoxDistance(const AbaxDataString& mk1,  const JagStrSplit& sp1, const AbaxDataString& mk2, const AbaxDataString& colType2,
										 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double a0 = jagatof( sp1[3].c_str() ); 
	double b0 = jagatof( sp1[4].c_str() ); 
	double c0 = jagatof( sp1[5].c_str() ); 
	double nx0 = safeget( sp1, 6 );
	double ny0 = safeget( sp1, 7 );

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return boxDistanceCube( srid, px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, r, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return boxDistanceBox( srid, px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return boxDistanceSphere( srid, px0, py0, pz0, a0, b0, c0, nx0, ny0, x, y, z, r, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return boxDistanceEllipsoid( srid, px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return boxDistanceCone( srid, px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, r,h, nx,ny, arg, dist );
	}
	return false;
}

double JagGeo::doTriangleArea( int srid1, const JagStrSplit& sp1 )
{
	double Ax = jagatof( sp1[0].c_str() );
	double Ay = jagatof( sp1[1].c_str() );
	double Bx = jagatof( sp1[2].c_str() );
	double By = jagatof( sp1[3].c_str() );
	double Cx = jagatof( sp1[4].c_str() );
	double Cy = jagatof( sp1[5].c_str() );
	return fabs( Ax*(By-Cy) + Bx*(Cy-Ay) + Cx*(Ay-By))/2.0;
}

bool JagGeo::doTriangleDistance(const AbaxDataString& mk1,  const JagStrSplit& sp1, const AbaxDataString& mk2, const AbaxDataString& colType2,
										 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	double x10 = jagatof( sp1[0].c_str() );
	double y10 = jagatof( sp1[1].c_str() );
	double x20 = jagatof( sp1[2].c_str() );
	double y20 = jagatof( sp1[3].c_str() );
	double x30 = jagatof( sp1[4].c_str() );
	double y30 = jagatof( sp1[5].c_str() );

	// like point Distance
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return triangleDistanceTriangle( srid, x10, y10, x20, y20, x30, y30, x1, y1, x2, y2, x3, y3, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3);
		return triangleDistanceSquare( srid, x10, y10, x20, y20, x30, y30, x0, y0, a, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4);
		return triangleDistanceRectangle( srid, x10, y10, x20, y20, x30, y30, x0, y0, a, b, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3);
		return triangleDistanceCircle( srid, x10, y10, x20, y20, x30, y30, x0, y0, r, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4);
		return triangleDistanceEllipse( srid, x10, y10, x20, y20, x30, y30, x0, y0, a, b, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return triangleDistancePolygon( srid, x10, y10, x20, y20, x30, y30, mk2, sp2, arg, dist );
	}
	return false;
}

double JagGeo::doTriangle3DArea( int srid1, const JagStrSplit& sp1 )
{
	double x1 = jagatof( sp1[0].c_str() );
	double y1 = jagatof( sp1[1].c_str() );
	double z1 = jagatof( sp1[2].c_str() );
	double x2 = jagatof( sp1[3].c_str() );
	double y2 = jagatof( sp1[4].c_str() );
	double z2 = jagatof( sp1[5].c_str() );
	double x3 = jagatof( sp1[6].c_str() );
	double y3 = jagatof( sp1[7].c_str() );
	double z3 = jagatof( sp1[8].c_str() );
	return sqrt( jagsq2(x2*y3-x3*y2) + jagsq2(x3*y1-x1*y3) + jagsq2(x1*y2-x2*y1) )/2.0;
}

bool JagGeo::doTriangle3DDistance(const AbaxDataString& mk1,  const JagStrSplit& sp1, const AbaxDataString& mk2, 
						const AbaxDataString& colType2, const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	double x10 = jagatof( sp1[0].c_str() );
	double y10 = jagatof( sp1[1].c_str() );
	double z10 = jagatof( sp1[2].c_str() );
	double x20 = jagatof( sp1[3].c_str() );
	double y20 = jagatof( sp1[4].c_str() );
	double z20 = jagatof( sp1[5].c_str() );
	double x30 = jagatof( sp1[6].c_str() );
	double y30 = jagatof( sp1[7].c_str() );
	double z30 = jagatof( sp1[8].c_str() );

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4);
		double ny = safeget( sp2, 5);
		return triangle3DDistanceCube( srid, x10,y10,z10,x20,y20,z20,x30,y30,z30, x0, y0, z0, r, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return triangle3DDistanceBox( srid, x10,y10,z10,x20,y20,z20,x30,y30,z30, x0, y0, z0, w,d,h, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return triangle3DDistanceSphere( srid, x10,y10,z10,x20,y20,z20,x30,y30,z30, x, y, z, r, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return triangle3DDistanceEllipsoid( srid, x10,y10,z10,x20,y20,z20,x30,y30,z30, x0, y0, z0, w,d,h, nx,ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5);
		double ny = safeget( sp2, 6);
		return triangle3DDistanceCone( srid, x10,y10,z10,x20,y20,z20,x30,y30,z30, x0, y0, z0, r,h, nx,ny, arg, dist );
	}
	return false;
}

double JagGeo::doCylinderArea( int srid1, const JagStrSplit& sp1 )
{
	double r = jagatof( sp1[3].c_str() ); 
	double c = jagatof( sp1[4].c_str() ); 
	return r*r*JAG_PI *c*2.0;
}

bool JagGeo::doCylinderDistance(const AbaxDataString& mk1,  const JagStrSplit& sp1, const AbaxDataString& mk2, const AbaxDataString& colType2,
										 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double pr0 = jagatof( sp1[3].c_str() ); 
	double c0 = jagatof( sp1[4].c_str() ); 

	double nx0 = safeget(sp1, 5);
	double ny0 = safeget(sp1, 6);

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return cylinderDistanceCube( srid, px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, r, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return cylinderDistanceBox( srid, px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return cylinderDistanceSphere( srid, px0, py0, pz0, pr0, c0,  nx0, ny0, x, y, z, r, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return cylinderDistanceEllipsoid( srid, px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return cylinderDistanceCone( srid, px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, r,h, nx,ny, arg, dist );
	}
	return false;
}

double JagGeo::doConeArea( int srid1, const JagStrSplit& sp1 )
{
	double r = jagatof( sp1[3].c_str() ); 
	double c = jagatof( sp1[4].c_str() ); 
	double R = r * 2.0;
	double h = c * 2.0;
	return JAG_PI * R * ( R + sqrt( h*h+ R*R) );
}

bool JagGeo::doConeDistance(const AbaxDataString& mk1,  const JagStrSplit& sp1, const AbaxDataString& mk2, const AbaxDataString& colType2,
										 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double pr0 = jagatof( sp1[3].c_str() ); 
	double c0 = jagatof( sp1[4].c_str() ); 
	double nx0 = safeget(sp1, 5 );
	double ny0 = safeget(sp1, 6 );

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return coneDistanceCube( srid, px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, r, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return coneDistanceBox( srid, px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return coneDistanceSphere( srid, px0, py0, pz0, pr0, c0,  nx0, ny0, x, y, z, r, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return coneDistanceEllipsoid( srid, px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return coneDistanceCone( srid, px0, py0, pz0, pr0, c0, nx0, ny0, x0, y0, z0, r,h, nx,ny, arg, dist );
	}
	return false;
}

double JagGeo::doEllipseArea( int srid1, const JagStrSplit& sp1 )
{
	double a = jagatof( sp1[2].c_str() ); 
	double b = jagatof( sp1[3].c_str() ); 
	return a*b*JAG_PI;
}
double JagGeo::doEllipse3DArea( int srid1, const JagStrSplit& sp1 )
{
	double a = jagatof( sp1[3].c_str() ); 
	double b = jagatof( sp1[4].c_str() ); 
	return a*b*JAG_PI;
}

bool JagGeo::doEllipseDistance(const AbaxDataString& mk1,  const JagStrSplit& sp1, const AbaxDataString& mk2, const AbaxDataString& colType2,
										 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double a0 = jagatof( sp1[2].c_str() ); 
	double b0 = jagatof( sp1[3].c_str() ); 
	double nx0 = safeget(sp1, 4);

	// like point Distance
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return ellipseDistanceTriangle( srid, px0, py0, a0, b0, nx0, x1, y1, x2, y2, x3, y3, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3);
		return ellipseDistanceSquare( srid, px0, py0, a0, b0, nx0, x0, y0, a, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4);
		return ellipseDistanceRectangle( srid, px0, py0, a0, b0, nx0, x0, y0, a, b, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3);
		return ellipseDistanceCircle( srid, px0, py0, a0, b0, nx0, x0, y0, r, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4);
		return ellipseDistanceEllipse( srid, px0, py0, a0, b0, nx0, x0, y0, a, b, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return ellipseDistancePolygon( srid, px0, py0, a0, b0, nx0, mk2, sp2, arg, dist );
	}
	return false;
}

double JagGeo::doEllipsoidArea( int srid1, const JagStrSplit& sp1 )
{
	double a = jagatof( sp1[3].c_str() ); 
	double b = jagatof( sp1[4].c_str() ); 
	double c = jagatof( sp1[5].c_str() ); 
	double p=1.6075; // Knud Thomsen approximation formula
	double ap = pow(a, p);
	double bp = pow(b, p);
	double cp = pow(b, p);
	double f = ( ap*(bp+cp)+bp*cp)/3.0;
	f = pow(f, 1.0/p);
	return 4.0*JAG_PI*f ;

}

bool JagGeo::doEllipsoidDistance(const AbaxDataString& mk1,  const JagStrSplit& sp1, const AbaxDataString& mk2, const AbaxDataString& colType2,
										 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	double px0 = jagatof( sp1[0].c_str() ); 
	double py0 = jagatof( sp1[1].c_str() ); 
	double pz0 = jagatof( sp1[2].c_str() ); 
	double a0 = jagatof( sp1[3].c_str() ); 
	double b0 = jagatof( sp1[4].c_str() ); 
	double c0 = jagatof( sp1[5].c_str() ); 
	double nx0 = safeget( sp1, 6);
	double ny0 = safeget( sp1, 7);

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4);
		double ny = safeget( sp2, 5);
		return ellipsoidDistanceCube( srid, px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, r, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return ellipsoidDistanceBox( srid, px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return ellipsoidDistanceSphere( srid, px0, py0, pz0, a0, b0, c0, nx0, ny0, x, y, z, r, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6);
		double ny = safeget( sp2, 7);
		return ellipsoidDistanceEllipsoid( srid, px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, w,d,h, nx,ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5);
		double ny = safeget( sp2, 6);
		return ellipsoidDistanceCone( srid, px0, py0, pz0, a0, b0, c0, nx0, ny0, x0, y0, z0, r,h, nx,ny, arg, dist );
	}
	return false;
}

bool JagGeo::doLineDistance(const AbaxDataString& mk1,  const JagStrSplit& sp1, const AbaxDataString& mk2, const AbaxDataString& colType2,
										 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	double x10 = jagatof( sp1[0].c_str() );
	double y10 = jagatof( sp1[1].c_str() );
	double x20 = jagatof( sp1[2].c_str() );
	double y20 = jagatof( sp1[3].c_str() );

	// like point Distance
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return lineDistanceTriangle( srid, x10, y10, x20, y20, x1, y1, x2, y2, x3, y3, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3 );
		return lineDistanceSquare( srid, x10, y10, x20, y20, x0, y0, a, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING ) {
		return lineDistanceLineString( srid, x10, y10, x20, y20, mk2, sp2, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		return lineDistanceRectangle( srid, x10, y10, x20, y20, x0, y0, a, b, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3 );
		return lineDistanceCircle( srid, x10, y10, x20, y20, x0, y0, r, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4 );
		return lineDistanceEllipse( srid, x10, y10, x20, y20, x0, y0, a, b, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return lineDistancePolygon( srid, x10, y10, x20, y20, mk2, sp2, arg, dist );
	}
	return false;
}

bool JagGeo::doLine3DDistance(const AbaxDataString& mk1,  const JagStrSplit& sp1, const AbaxDataString& mk2, const AbaxDataString& colType2,
										 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	double x10 = jagatof( sp1[0].c_str() );
	double y10 = jagatof( sp1[1].c_str() );
	double z10 = jagatof( sp1[2].c_str() );
	double x20 = jagatof( sp1[3].c_str() );
	double y20 = jagatof( sp1[4].c_str() );
	double z20 = jagatof( sp1[5].c_str() );

	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return line3DDistanceCube( srid, x10,y10,z10,x20,y20,z20, x0, y0, z0, r, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return line3DDistanceLineString3D( srid, x10,y10,z10,x20,y20,z20, mk2, sp2, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return line3DDistanceBox( srid, x10,y10,z10,x20,y20,z20, x0, y0, z0, w,d,h, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return line3DDistanceSphere( srid, x10,y10,z10,x20,y20,z20, x, y, z, r, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return line3DDistanceEllipsoid( srid, x10,y10,z10,x20,y20,z20, x0, y0, z0, w,d,h, nx,ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return line3DDistanceCone( srid, x10,y10,z10,x20,y20,z20, x0, y0, z0, r,h, nx,ny, arg, dist );
	}
	return false;
}

bool JagGeo::doLineStringDistance(const AbaxDataString& mk1, const JagStrSplit& sp1, const AbaxDataString& mk2, const AbaxDataString& colType2,
										 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{

	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		// JAG_C_COL_TYPE_TRIANGLE is 2D already
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return lineStringDistanceTriangle( srid, mk1, sp1, x1, y1, x2, y2, x3, y3, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING ) {
		return lineStringDistanceLineString( srid, mk1, sp1, mk2, sp2, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3 );
		//prt(("s0881 lineStringDistanceSquare ...\n" ));
		return lineStringDistanceSquare( srid, mk1, sp1, x0, y0, a, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		return lineStringDistanceRectangle( srid, mk1, sp1, x0, y0, a, b, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3 );
		return lineStringDistanceCircle( srid, mk1, sp1, x0, y0, r, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4 );
		return lineStringDistanceEllipse( srid, mk1, sp1, x0, y0, a, b, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return lineStringDistancePolygon( srid, mk1, sp1, mk2, sp2, arg, dist );
	}
	return false;
}

bool JagGeo::doLineString3DDistance(const AbaxDataString& mk1, const JagStrSplit& sp1, const AbaxDataString& mk2, const AbaxDataString& colType2,
										 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	if ( colType2 == JAG_C_COL_TYPE_POINT3D ) {
		return doPoint3DDistance( mk2, sp2, mk1, JAG_C_COL_TYPE_LINESTRING3D, sp1, srid,  arg, dist);
	} else if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return lineString3DDistanceCube( srid, mk1, sp1, x0, y0, z0, r, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_LINESTRING3D ) {
		return lineString3DDistanceLineString3D( srid, mk1, sp1, mk2, sp2, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return lineString3DDistanceBox( srid, mk1, sp1, x0, y0, z0, w,d,h, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return lineString3DDistanceSphere( srid, mk1, sp1, x, y, z, r, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return lineString3DDistanceEllipsoid( srid, mk1, sp1, x0, y0, z0, w,d,h, nx,ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return lineString3DDistanceCone( srid, mk1, sp1, x0, y0, z0, r,h, nx,ny, arg, dist );
	}
	return false;
}

bool JagGeo::doPolygonDistance(const AbaxDataString& mk1, const JagStrSplit& sp1, const AbaxDataString& mk2, const AbaxDataString& colType2,
										 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return polygonDistanceTriangle( srid, mk1, sp1, x1, y1, x2, y2, x3, y3, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3 );
		return polygonDistanceSquare( srid, mk1, sp1, x0, y0, a, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		return polygonDistanceRectangle( srid, mk1, sp1, x0, y0, a, b, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3 );
		return polygonDistanceCircle( srid, mk1, sp1, x0, y0, r, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4 );
		return polygonDistanceEllipse( srid, mk1, sp1, x0, y0, a, b, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return polygonDistancePolygon( srid, mk1, sp1, mk2, sp2, arg, dist );
	}
	return false;
}

bool JagGeo::doPolygon3DDistance(const AbaxDataString& mk1, const JagStrSplit& sp1, const AbaxDataString& mk2, const AbaxDataString& colType2,
										 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return polygon3DDistanceCube( srid, mk1, sp1, x0, y0, z0, r, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return polygon3DDistanceBox( srid, mk1, sp1, x0, y0, z0, w,d,h, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return polygon3DDistanceSphere( srid, mk1, sp1, x, y, z, r, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return polygon3DDistanceEllipsoid( srid, mk1, sp1, x0, y0, z0, w,d,h, nx,ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return polygon3DDistanceCone( srid, mk1, sp1, x0, y0, z0, r,h, nx,ny, arg, dist );
	}
	return false;
}

bool JagGeo::doMultiPolygonDistance(const AbaxDataString& mk1, const JagStrSplit& sp1, const AbaxDataString& mk2, const AbaxDataString& colType2,
										 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	if (  colType2 == JAG_C_COL_TYPE_TRIANGLE ) {
		double x1 = jagatof( sp2[0].c_str() );
		double y1 = jagatof( sp2[1].c_str() );
		double x2 = jagatof( sp2[2].c_str() );
		double y2 = jagatof( sp2[3].c_str() );
		double x3 = jagatof( sp2[4].c_str() );
		double y3 = jagatof( sp2[5].c_str() );
		return multiPolygonDistanceTriangle( srid, mk1, sp1, x1, y1, x2, y2, x3, y3, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SQUARE ) {
		prt(("s6040 JAG_C_COL_TYPE_SQUARE sp2 print():\n"));
		//sp2.print();
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3 );
		return multiPolygonDistanceSquare( srid, mk1, sp1, x0, y0, a, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_RECTANGLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() ); 
		double b = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		return multiPolygonDistanceRectangle( srid, mk1, sp1, x0, y0, a, b, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CIRCLE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double r = jagatof( sp2[2].c_str() ); 
		double nx = safeget( sp2, 3 );
		return multiPolygonDistanceCircle( srid, mk1, sp1, x0, y0, r, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double a = jagatof( sp2[2].c_str() );
		double b = jagatof( sp2[3].c_str() );
		double nx = safeget( sp2, 4 );
		return multiPolygonDistanceEllipse( srid, mk1, sp1, x0, y0, a, b, nx, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_POLYGON ) {
		return multiPolygonDistancePolygon( srid, mk1, sp1, mk2, sp2, arg, dist );
	}
	return false;
}

bool JagGeo::doMultiPolygon3DDistance(const AbaxDataString& mk1, const JagStrSplit& sp1, const AbaxDataString& mk2, const AbaxDataString& colType2,
										 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist)
{
	if (  colType2 == JAG_C_COL_TYPE_CUBE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() ); 
		double nx = safeget( sp2, 4 );
		double ny = safeget( sp2, 5 );
		return multiPolygon3DDistanceCube( srid, mk1, sp1, x0, y0, z0, r, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_BOX ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() ); 
		double d = jagatof( sp2[4].c_str() ); 
		double h = jagatof( sp2[5].c_str() ); 
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return multiPolygon3DDistanceBox( srid, mk1, sp1, x0, y0, z0, w,d,h, nx, ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_SPHERE ) {
		double x = jagatof( sp2[0].c_str() ); 
		double y = jagatof( sp2[1].c_str() ); 
		double z = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		return multiPolygon3DDistanceSphere( srid, mk1, sp1, x, y, z, r, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_ELLIPSOID ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double w = jagatof( sp2[3].c_str() );
		double d = jagatof( sp2[4].c_str() );
		double h = jagatof( sp2[5].c_str() );
		double nx = safeget( sp2, 6 );
		double ny = safeget( sp2, 7 );
		return multiPolygon3DDistanceEllipsoid( srid, mk1, sp1, x0, y0, z0, w,d,h, nx,ny, arg, dist );
	} else if ( colType2 == JAG_C_COL_TYPE_CONE ) {
		double x0 = jagatof( sp2[0].c_str() ); 
		double y0 = jagatof( sp2[1].c_str() ); 
		double z0 = jagatof( sp2[2].c_str() ); 
		double r = jagatof( sp2[3].c_str() );
		double h = jagatof( sp2[4].c_str() );
		double nx = safeget( sp2, 5 );
		double ny = safeget( sp2, 6 );
		return multiPolygon3DDistanceCone( srid, mk1, sp1, x0, y0, z0, r,h, nx,ny, arg, dist );
	}
	return false;
}



// 2D point
bool JagGeo::pointDistancePoint( int srid, double px, double py, double x1, double y1, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px, py, x1, y1, srid );
    return true;
}

bool JagGeo::pointDistanceLine( int srid, double px, double py, double x1, double y1, double x2, double y2, 
								const AbaxDataString& arg, double &dist )
{
	bool rc = true;
	if ( arg.caseEqual( "min" ) ) {
		double d1 = JagGeo::distance( px, py, x1, y1, srid );
		double d2 = JagGeo::distance( px, py, x2, y2, srid );
		dist = jagmin( d1, d2 );
	} else if ( arg.caseEqual( "max" ) ) {
		double d1 = JagGeo::distance( px, py, x1, y1, srid );
		double d2 = JagGeo::distance( px, py, x2, y2, srid );
		dist = jagmax( d1, d2 );
	} else if ( arg.caseEqual( "perpendicular" ) ) {
		if ( jagEQ(y1,y2) && jagEQ(x1,x2) ) return false;
		if ( 0 == srid ) {
			dist = fabs( (y2-y1)*px - (x2-x1)*py + x2*y1 - y2*x1 )/ sqrt( (y2-y1)*(y2-y1) + (x2-x1)*(x2-x1) );
		} else if ( JAG_GEO_WGS84 == srid ) {
			dist = pointToLineDistance( y1, x1, y2, x2, py, px );
		} else {
			dist = 0;
			return false;
		}
	} 

    return rc;
}

// arg: "min" "max"
// dist in number or meters
bool JagGeo::pointDistanceLineString( int srid,  double x, double y, const AbaxDataString &mk2, const JagStrSplit &sp2, 
									  const AbaxDataString& arg, double &dist )
{
	int start = 0;
	if ( mk2 == JAG_OJAG ) {
		start = 1;
	}

	if ( arg.caseEqual( "center" ) ) {
		double avgx, avgy;
		bool rc = lineStringAverage( mk2, sp2, avgx, avgy );
		if ( ! rc ) { 
			dist = 0.0;
			return false;
		}
		dist = JagGeo::distance( x, y, avgx, avgy, srid );
		return true;
	}

    double dx, dy, d;
	double mind = LONG_MAX;
	double maxd = LONG_MIN;
    const char *str;
    char *p;
	for ( int i=start; i < sp2.length(); ++i ) {
		str = sp2[i].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx, dy );
		d = JagGeo::distance( x, y, dx, dy, srid );
		if ( d < mind ) mind = d;
		if ( d > maxd ) maxd = d;
	}

	if ( arg.caseEqual( "max" ) ) {
		dist = maxd;
	} else if ( arg.caseEqual( "min" ) ) {
		dist = mind;
	} else {
		return false;
	}

	return true;
}

// min max center
bool JagGeo::pointDistanceSquare( int srid, double x, double y, double px0, double py0, double r, double nx, 
								  const AbaxDataString& arg, double &dist )
{
    return pointDistanceRectangle( srid, x, y, px0, py0, r, r, nx, arg, dist );
}

// min max center
bool JagGeo::pointDistanceCircle(int srid, double px, double py, double x0, double y0, double r, const AbaxDataString& arg, double &dist )
{
	double d;
	d = JagGeo::distance( px, py, x0, y0, srid );
	if ( arg.caseEqual("center") ) {
		dist = d;
		return true;
	}

	if ( arg.caseEqual("max") ) {
		dist = r + d;
	} else {
		if ( r > d ) dist = r-d;
		else dist = d - r;
	}
	return true;
}

// min max center
bool JagGeo::pointDistanceRectangle( int srid, double x, double y, double px0, double py0, double a0, double b0, double nx0, 
									 const AbaxDataString& arg, double &dist )
{
	if ( arg.caseEqual("center") ) {
		dist = JagGeo::distance( x, y, px0, py0, srid );
		return true;
	}

	double d, sqx[4], sqy[4];
	double mind = LONG_MAX;
	double maxd = LONG_MIN;
	transform2DCoordLocal2Global( px0, py0, -a0, -b0, nx0, sqx[0], sqy[0] );
	transform2DCoordLocal2Global( px0, py0, -a0, b0, nx0, sqx[1], sqy[1] );
	transform2DCoordLocal2Global( px0, py0, a0, b0, nx0, sqx[2], sqy[2] );
	transform2DCoordLocal2Global( px0, py0, a0, -b0, nx0, sqx[3], sqy[3] );
	for ( int i=0; i < 4; ++i ) {
		d = JagGeo::distance( x, y, sqx[i], sqy[i], srid );
		if ( d < mind ) mind = d;
		if ( d > maxd ) maxd = d;
	}

	if ( arg.caseEqual("max") ) {
		dist = maxd;
	} else {
		dist = mind;
	}
    return true;
}

bool JagGeo::pointDistanceEllipse( int srid, double px, double py, double x, double y, double a, double b, double nx, 
								   const AbaxDataString& arg, double &dist )
{

	// todo001
	dist = JagGeo::distance( x, y, px, py, srid );
	if ( arg.caseEqual("center") ) {
		return true;
	}

	if ( arg.caseEqual("max") ) {
	} else {
	}

    return true;
}

// min max
bool JagGeo::pointDistanceTriangle(int srid, double px, double py, double x1, double y1,
								  double x2, double y2, double x3, double y3,
				 				  const AbaxDataString& arg, double &dist )
{
	double d1, d2, d3;
	d1 = JagGeo::distance( x1, y1, px, py, srid );
	d2 = JagGeo::distance( x2, y2, px, py, srid );
	d3 = JagGeo::distance( x3, y3, px, py, srid );
	if ( arg.caseEqual("max") ) {
		dist = jagmax3(d1, d2, d3);
	} else {
		dist = jagmin3(d1, d2, d3);
	}

    return true;
}

bool JagGeo::pointDistancePolygon( int srid, double x, double y, const AbaxDataString &mk2, const JagStrSplit &sp2, 
								   const AbaxDataString& arg, double &dist )
{
    const char *str;
    char *p;
	JagPolygon pgon;
	int rc;
	if ( mk2 == JAG_OJAG ) {
		//prt(("s8123 JAG_OJAG sp2: prnt\n" ));
		//sp2.print();
		rc = JagParser::addPolygonData( pgon, sp2, true );
	} else {
		// form linesrting3d  from pdata
		p = secondTokenStart( sp2.c_str() );
		//prt(("s8110 p=[%s]\n", p ));
		rc = JagParser::addPolygonData( pgon, p, true, false );
	}

	if ( rc < 0 ) {
		//prt(("s8112 rc=%d false\n", rc ));
		return false;
	}

	double mind = LONG_MAX;
	double maxd = LONG_MIN;
	double d;
	const JagLineString3D &linestr = pgon.linestr[0];
	for ( int i=0; i < linestr.size(); ++i ) {
		d = JagGeo::distance( x, y, linestr.point[i].x, linestr.point[i].y, srid );
		if ( d > maxd ) maxd = d;
		if ( d < mind ) mind = d;
	}

	if ( arg.caseEqual("max") ) {
		dist = maxd;
	} else {
		dist = mind;
	}

	return true;
}


// 3D point
bool JagGeo::point3DDistancePoint3D( int srid, double px, double py, double pz, double x1, double y1, double z1, 
									 const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px, py, pz, x1, y1, z1, srid );
	prt(("s2083 px=%f py=%f pz=%f srid=%d\n", px, py, pz, srid ));
	prt(("s3028  x1, y1, z1= %f %f %f\n",  x1, y1, z1 ));
    return true;
}
bool JagGeo::point3DDistanceLine3D(int srid, double px, double py, double pz, double x1, double y1, double z1, 
								double x2, double y2, double z2, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px, py, pz, x1, y1, z1, srid );
	double dist2 = JagGeo::distance( px, py, pz, x2, y2, z2, srid );

	if ( arg.caseEqual("max") ) {
		dist = jagmax( dist, dist2 );
	} else {
		dist = jagmin( dist, dist2 );
	}

    return true;
}
bool JagGeo::point3DDistanceLineString3D(int srid,  double x, double y, double z, const AbaxDataString &mk2, 
										const JagStrSplit &sp2, const AbaxDataString& arg, double &dist )
{
	prt(("s2038 srid=%d x y z = %f %f %f\n", srid, x, y, z ));
	//prt(("s5780 sp2:\n" ));
	//sp2.print(); 
	// _OJAG_=0=test.linestr3d.l3=LS3=0 1.0:2.0:3.0:5.0:6.0:7.0 1.0:2.0:3.0 2.0:3.0:4.0 5.0:6.0:7.0
    double dx, dy, dz;
    const char *str;
    char *p;
	double max = LONG_MIN;
	double min = LONG_MAX;
	bool isMax;
	if ( arg.caseEqual("max") ) { isMax = true;	} else { isMax = false;	}
	for ( int i = 2; i < sp2.length(); ++i ) {
		str = sp2[i].c_str();
		if ( strchrnum( str, ':') < 2 ) continue;
		get3double(str, p, ':', dx, dy, dz );
		dist = JagGeo::distance( dx, dy, dz, x, y, z, srid );
		if ( isMax ) {
			if ( dist > max ) max = dist;
		} else {
			if ( dist < min ) min = dist;
		}
	}

	if ( isMax ) { dist = max; }
	else { dist = min; }

    return true;
}
//dx dy dz are the point3d's coordinate;
//d == a; w == b; h == c;
bool JagGeo::point3DDistanceBox(int srid, double dx, double dy, double dz,
								  double x0, double y0, double z0, double d, double w, double h, double nx, double ny,
								  const AbaxDataString& arg, double &dist )
{
    double px, py, pz, maxd1, maxd2, maxd3, mind1;
    double xsum = 0, ysum = 0, zsum = 0;
    long counter = 0;
    double mind = LONG_MAX;
    double maxd = LONG_MIN;
    const char *str;
    char *p;

    transform3DCoordGlobal2Local( x0, y0, z0, dx, dy, dz, nx, ny, px, py, pz );
    if (fabs(px) <= d && fabs(py) <= w){
        //point to up and down sides
        mind1 = fabs(fabs(pz) - h);
        maxd1 = distance( fabs(px), fabs(py), fabs(pz), -d, -w, -h, srid );
        if ( mind1 < mind ) mind = mind1;
        if ( maxd1 > maxd ) maxd = maxd1;
    }else if(fabs(py) <= w && fabs(pz) < h){
        //point to front and back sides
        mind1 = fabs(fabs(px) - d);
        maxd1 = distance( fabs(px), fabs(py), fabs(pz), -d, -w, -h, srid );
        if ( mind1 < mind ) mind = mind1;
        if ( maxd1 > maxd ) maxd = maxd1;
    }else if(fabs(px) <= d && fabs(pz) < h){
        //point to left and right sides
        mind1 = fabs(fabs(py) - w);
        maxd1 = distance( fabs(px), fabs(py), fabs(pz), -d, -w, -h, srid );
        if ( mind1 < mind ) mind = mind1;
        if ( maxd1 > maxd ) maxd = maxd1;
    }else if(fabs(px) <= d){
        //point to 4 depth lines
        mind1 = DistanceOfPointToLine(fabs(px), fabs(py), fabs(pz), d, w, h, -d, w, h);
        maxd1 = distance( fabs(px), fabs(py), fabs(pz), -d, -w, -h, srid );
        if ( mind1 < mind ) mind = mind1;
        if ( maxd1 > maxd ) maxd = maxd1;
    }else if(fabs(py) <= w){
        //point to 4 width lines
        mind1 = DistanceOfPointToLine(fabs(px), fabs(py), fabs(pz), d, w, h, d, -w, h);
        maxd1 = distance( fabs(px), fabs(py), fabs(pz), -d, -w, -h, srid );
        if ( mind1 < mind ) mind = mind1;
        if ( maxd1 > maxd ) maxd = maxd1;
    }else if(fabs(pz) <= h){
        //point to 4 height lines
        mind1 = DistanceOfPointToLine(fabs(px), fabs(py), fabs(pz), d, w, h, d, w, -h);
        maxd1 = distance( fabs(px), fabs(py), fabs(pz), -d, -w, -h, srid );
        if ( mind1 < mind ) mind = mind1;
        if ( maxd1 > maxd ) maxd = maxd1;
    }else{
        //point to 8 points
        mind1 = distance( fabs(px), fabs(py), fabs(pz), d, w, h, srid );
        maxd1 = distance( fabs(px), fabs(py), fabs(pz), -d, -w, -h, srid );
        if ( mind1 < mind ) mind = mind1;
        if ( maxd1 > maxd ) maxd = maxd1;
    }

    if ( arg.caseEqual( "max" ) ) {
        dist = maxd;
    } else if (arg.caseEqual("min" )){
        dist = mind;
    }else {
        dist = distance( dx, dy, dz, x0, y0, z0, srid );
    }
    return true;
}
bool JagGeo::point3DDistanceSphere(int srid, double px, double py, double pz, double x, double y, double z, double r, 
									const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px, py, pz, x, y, z, srid );
    return true;
}
bool JagGeo::point3DDistanceEllipsoid(int srid, double px, double py, double pz,  
								  double x, double y, double z, double a, double b, double c, double nx, double ny, 
								  const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px, py, pz, x, y, z, srid );
    return true;
}

bool JagGeo::point3DDistanceCone(int srid, double px, double py, double pz, 
								double x, double y, double z,
								 double r, double h,  double nx, double ny, const AbaxDataString& arg, double &dist )
{
	// transform px py pz to local normal cone
	double lx, ly, lz; // x y z is coord of cone's center
	transform3DCoordGlobal2Local( x,y,z, px,py,pz, nx, ny, lx, ly, lz );
	return point3DDistanceNormalCone(srid, lx, ly, lz, r, h, arg, dist ); 
}
bool JagGeo::point3DDistanceSquare3D(int srid, double px, double py, double pz, 
								double x, double y, double z,
								 double a, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px, py, pz, x, y, z, srid );
    return true;
}
bool JagGeo::point3DDistanceCylinder(int srid,  double px, double py, double pz,
                                    double x, double y, double z,
                                    double r, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px, py, pz, x, y, z, srid );
    return true;
}

// 2D circle
bool JagGeo::circleDistanceCircle(int srid, double px, double py, double pr, double x, double y, double r, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px, py, x, y, srid );
    return true;
}

bool JagGeo::circleDistanceSquare( int srid, double px0, double py0, double pr, double x0, double y0, double r, 
								   double nx, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px0, py0, x0, y0, srid );
    return true;
}

bool JagGeo::circleDistanceEllipse(int srid, double px, double py, double pr, 
								 	 double x, double y, double w, double h, double nx, 
								 	 const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px, py, x, y, srid );
    return true;
}
bool JagGeo::circleDistanceRectangle(int srid, double px0, double py0, double pr, double x0, double y0,
									  double w, double h, double nx,  const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px0, py0, x0, y0, srid );
    return true;
}

bool JagGeo::circleDistanceTriangle(int srid, double px, double py, double pr, double x1, double y1, 
								  double x2, double y2, double x3, double y3, const AbaxDataString& arg, double &dist ) 
{
	dist = JagGeo::distance( px, py, x1, y1, srid );
	double dist2 = JagGeo::distance( px, py, x2, y2, srid );
	double dist3 = JagGeo::distance( px, py, x3, y3, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax3( dist, dist2, dist3 );
	} else {
		dist = jagmin3( dist, dist2, dist3 );
	}
    return true;
}

bool JagGeo::circleDistancePolygon(int srid, double px0, double py0, double pr, 
								 const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist )
{
	prt(("s2030 sp2:\n" ));
	// sp2.print();
	// [_OJAG_=0=test.pol1.pol=PL=0 0.0:0.0:80.0:80.0 0.0:0.0 80.0:0.0 80.0:80.0 0.0:80.0 0.0:0.0
    double dx, dy;
    const char *str;
    char *p;
	double max = LONG_MIN;
	double min = LONG_MAX;
	bool isMax;
	if ( arg.caseEqual("max") ) { isMax = true;	} else { isMax = false;	}
	for ( int i = 2; i < sp2.length(); ++i ) {
		str = sp2[i].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx, dy );
		dist = JagGeo::distance( dx, dy, px0, py0, srid );
		if ( isMax ) {
			if ( dist > max ) max = dist;
		} else {
			if ( dist < min ) min = dist;
		}
	}

	if ( isMax ) { dist = max; }
	else { dist = min; }

    return true;
}

// 3D circle
bool JagGeo::circle3DDistanceCube(int srid, double px0, double py0, double pz0, double pr0,  double nx0, double ny0,
									double x0, double y0, double z0,  double r, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px0, py0, pz0, x0, y0, z0, srid );
    return true;
}

bool JagGeo::circle3DDistanceBox(int srid, double px0, double py0, double pz0, double pr0,  double nx0, double ny0,
				                   double x0, double y0, double z0,  double a, double b, double c,
				                   double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px0, py0, pz0, x0, y0, z0, srid );
    return true;
}

bool JagGeo::circle3DDistanceSphere(int srid, double px0, double py0, double pz0, double pr0,   double nx0, double ny0,
   									 double x, double y, double z, double r, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px0, py0, pz0, x, y, z, srid );
    return true;
}

bool JagGeo::circle3DDistanceEllipsoid(int srid, double px0, double py0, double pz0, double pr0,  double nx0, double ny0,
									  double x0, double y0, double z0, 
								 	   double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px0, py0, pz0, x0, y0, z0, srid );
    return true;
}
bool JagGeo::circle3DDistanceCone(int srid, double px0, double py0, double pz0, double pr0, double nx0, double ny0,
									  double x0, double y0, double z0, 
								 	   double r, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px0, py0, pz0, x0, y0, z0, srid );
    return true;
}

// 3D sphere
bool JagGeo::sphereDistanceCube(int srid,  double px0, double py0, double pz0, double pr0,
	                               double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px0, py0, pz0, x0, y0, z0, srid );
    return true;
}

bool JagGeo::sphereDistanceBox(int srid,  double px0, double py0, double pz0, double r,
	                                double x0, double y0, double z0, double w, double d, double h, 
									double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px0, py0, pz0, x0, y0, z0, srid );
    return true;
}

bool JagGeo::sphereDistanceSphere(int srid,  double px0, double py0, double pz0, double pr, 
									double x, double y, double z, double r, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px0, py0, pz0, x, y, z, srid );
    return true;
}
bool JagGeo::sphereDistanceEllipsoid(int srid,  double px0, double py0, double pz0, double pr,
	                                    double x0, double y0, double z0, double w, double d, double h, 
										double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px0, py0, pz0, x0, y0, z0, srid );
    return true;
}
bool JagGeo::sphereDistanceCone(int srid,  double px0, double py0, double pz0, double pr,
	                                    double x0, double y0, double z0, double r, double h, 
										double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px0, py0, pz0, x0, y0, z0, srid );
    return true;
}

// 2D rectangle
bool JagGeo::rectangleDistanceTriangle(int srid, double px, double py, double a0, double b0, double nx0, 
									 double x1, double y1, double x2, double y2, double x3, double y3, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px, py, x1, y1, srid );
	double dist2 = JagGeo::distance( px, py, x2, y2, srid );
	double dist3 = JagGeo::distance( px, py, x3, y3, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax3( dist, dist2, dist3 );
	} else {
		dist = jagmin3( dist, dist2, dist3 );
	}
    return true;
}

bool JagGeo::rectangleDistanceSquare(int srid, double px0, double py0, double a0, double b0, double nx0,
	                                double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px0, py0, x0, y0, srid );
    return true;
}
bool JagGeo::rectangleDistanceRectangle(int srid, double px0, double py0, double a0, double b0, double nx0,
	                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px0, py0, x0, y0, srid );
    return true;
}

bool JagGeo::rectangleDistanceEllipse(int srid, double px0, double py0, double a0, double b0, double nx0,
	                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px0, py0, x0, y0, srid );
    return true;
}

bool JagGeo::rectangleDistanceCircle(int srid, double px0, double py0, double a0, double b0, double nx0, 
								    double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( px0, py0, x0, y0, srid );
    return true;
}

bool JagGeo::rectangleDistancePolygon(int srid, double px0, double py0, double a0, double b0, double nx0, 
									const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist )
{
    double dx, dy;
    const char *str;
    char *p;
	double max = LONG_MIN;
	double min = LONG_MAX;
	bool isMax;
	if ( arg.caseEqual("max") ) { isMax = true;	} else { isMax = false;	}
	for ( int i = 2; i < sp2.length(); ++i ) {
		str = sp2[i].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx, dy );
		dist = JagGeo::distance( dx, dy, px0, py0, srid );
		if ( isMax ) {
			if ( dist > max ) max = dist;
		} else {
			if ( dist < min ) min = dist;
		}
	}

	if ( isMax ) { dist = max; }
	else { dist = min; }
    return true;
}

// 2D triangle
bool JagGeo::triangleDistanceTriangle(int srid, double x10, double y10, double x20, double y20, double x30, double y30,
									 double x1, double y1, double x2, double y2, double x3, double y3, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x10, y10, x1, y1, srid );
	double dist2 = JagGeo::distance( x20, y20, x2, y2, srid );
	double dist3 = JagGeo::distance( x30, y30, x3, y3, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax3( dist, dist2, dist3 );
	} else {
		dist = jagmin3( dist, dist2, dist3 );
	}
    return true;
}

bool JagGeo::triangleDistanceSquare(int srid, double x10, double y10, double x20, double y20, double x30, double y30,
	                                double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x10, y10, x0, y0, srid );
	double dist2 = JagGeo::distance( x20, y20, x0, y0, srid );
	double dist3 = JagGeo::distance( x30, y30, x0, y0, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax3( dist, dist2, dist3 );
	} else {
		dist = jagmin3( dist, dist2, dist3 );
	}
    return true;
}
bool JagGeo::triangleDistanceRectangle(int srid, double x10, double y10, double x20, double y20, double x30, double y30,
	                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x10, y10, x0, y0, srid );
	double dist2 = JagGeo::distance( x20, y20, x0, y0, srid );
	double dist3 = JagGeo::distance( x30, y30, x0, y0, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax3( dist, dist2, dist3 );
	} else {
		dist = jagmin3( dist, dist2, dist3 );
	}
    return true;
}
bool JagGeo::triangleDistanceEllipse(int srid, double x10, double y10, double x20, double y20, double x30, double y30,
	                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x10, y10, x0, y0, srid );
	double dist2 = JagGeo::distance( x20, y20, x0, y0, srid );
	double dist3 = JagGeo::distance( x30, y30, x0, y0, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax3( dist, dist2, dist3 );
	} else {
		dist = jagmin3( dist, dist2, dist3 );
	}
    return true;
}

bool JagGeo::triangleDistanceCircle(int srid, double x10, double y10, double x20, double y20, double x30, double y30,
								    double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x10, y10, x0, y0, srid );
	double dist2 = JagGeo::distance( x20, y20, x0, y0, srid );
	double dist3 = JagGeo::distance( x30, y30, x0, y0, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax3( dist, dist2, dist3 );
	} else {
		dist = jagmin3( dist, dist2, dist3 );
	}
    return true;
}

bool JagGeo::triangleDistancePolygon(int srid, double x10, double y10, double x20, double y20, double x30, double y30,
								    const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist )
{
    double dx, dy;
    const char *str;
    char *p;
	double max = LONG_MIN;
	double min = LONG_MAX;
	double dist2, dist3;
	bool isMax;
	if ( arg.caseEqual("max") ) { isMax = true;	} else { isMax = false;	}
	for ( int i = 2; i < sp2.length(); ++i ) {
		str = sp2[i].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx, dy );
		dist = JagGeo::distance( dx, dy, x10, y10, srid );
		dist2 = JagGeo::distance( dx, dy, x20, y20, srid );
		dist3 = JagGeo::distance( dx, dy, x30, y30, srid );
		if ( isMax ) {
			dist = jagmax3( dist, dist2, dist3 );
			if ( dist > max ) max = dist;
		} else {
			dist = jagmin3( dist, dist2, dist3 );
			if ( dist < min ) min = dist;
		}
	}

	if ( isMax ) { dist = max; }
	else { dist = min; }
    return true;
}
									
// 2D ellipse
bool JagGeo::ellipseDistanceTriangle(int srid, double px0, double py0, double a0, double b0, double nx0, 
									 double x1, double y1, double x2, double y2, double x3, double y3, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x1, y1, px0, py0, srid );
	double dist2 = JagGeo::distance( x2, y2, px0, py0, srid );
	double dist3 = JagGeo::distance( x3, y3, px0, py0, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax3( dist, dist2, dist3 );
	} else {
		dist = jagmin3( dist, dist2, dist3 );
	}
    return true;
}

bool JagGeo::ellipseDistanceSquare(int srid, double px0, double py0, double a0, double b0, double nx0,
	                                double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, px0, py0, srid );
    return true;
}
bool JagGeo::ellipseDistanceRectangle(int srid, double px0, double py0, double a0, double b0, double nx0,
	                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, px0, py0, srid );
    return true;
}
bool JagGeo::ellipseDistanceEllipse(int srid, double px0, double py0, double a0, double b0, double nx0,
	                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, px0, py0, srid );
    return true;
}

bool JagGeo::ellipseDistanceCircle(int srid, double px0, double py0, double a0, double b0, double nx0, 
								    double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, px0, py0, srid );
    return true;
}

bool JagGeo::ellipseDistancePolygon(int srid, double px0, double py0, double a0, double b0, double nx0, 
								    const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist )
{
    double dx, dy;
    const char *str;
    char *p;
	double max = LONG_MIN;
	double min = LONG_MAX;
	bool isMax;
	if ( arg.caseEqual("max") ) { isMax = true;	} else { isMax = false;	}
	for ( int i = 2; i < sp2.length(); ++i ) {
		str = sp2[i].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx, dy );
		dist = JagGeo::distance( dx, dy, px0, py0, srid );
		if ( isMax ) {
			if ( dist > max ) max = dist;
		} else {
			if ( dist < min ) min = dist;
		}
	}

	if ( isMax ) { dist = max; }
	else { dist = min; }
    return true;
}

// rect 3D
bool JagGeo::rectangle3DDistanceCube(int srid,  double px0, double py0, double pz0, double a0, double b0, double nx0, double ny0,
                                double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}

bool JagGeo::rectangle3DDistanceBox(int srid,  double px0, double py0, double pz0, double a0, double b0,
                                double nx0, double ny0,
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}
bool JagGeo::rectangle3DDistanceSphere(int srid,  double px0, double py0, double pz0, double a0, double b0,
                                       double nx0, double ny0,
                                       double x, double y, double z, double r, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x, y, z, px0, py0, pz0, srid );
    return true;
}
bool JagGeo::rectangle3DDistanceEllipsoid(int srid,  double px0, double py0, double pz0, double a0, double b0,
                                    double nx0, double ny0,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}
bool JagGeo::rectangle3DDistanceCone(int srid,  double px0, double py0, double pz0, double a0, double b0,
                                    double nx0, double ny0,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}


// triangle 3D
bool JagGeo::triangle3DDistanceCube(int srid,  double x10, double y10, double z10, double x20, double y20, double z20,
								double x30, double y30,  double z30,
								double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x10, y10, z10, x0, y0, z0, srid );
	double dist2 = JagGeo::distance( x20, y20, z20, x0, y0, z0, srid );
	double dist3 = JagGeo::distance( x30, y30, z30, x0, y0, z0, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax3( dist, dist2, dist3 );
	} else {
		dist = jagmin3( dist, dist2, dist3 );
	}
    return true;
}

bool JagGeo::triangle3DDistanceBox(int srid,  double x10, double y10, double z10, double x20, double y20, double z20, 
								double x30, double y30, double z30,
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x10, y10, z10, x0, y0, z0, srid );
	double dist2 = JagGeo::distance( x20, y20, z20, x0, y0, z0, srid );
	double dist3 = JagGeo::distance( x30, y30, z30, x0, y0, z0, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax3( dist, dist2, dist3 );
	} else {
		dist = jagmin3( dist, dist2, dist3 );
	}
    return true;
}
bool JagGeo::triangle3DDistanceSphere(int srid,  double x10, double y10, double z10, double x20, double y20, double z20,
								   double x30, double y30, double z30,
                                       double x0, double y0, double z0, double r, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x10, y10, z10, x0, y0, z0, srid );
	double dist2 = JagGeo::distance( x20, y20, z20, x0, y0, z0, srid );
	double dist3 = JagGeo::distance( x30, y30, z30, x0, y0, z0, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax3( dist, dist2, dist3 );
	} else {
		dist = jagmin3( dist, dist2, dist3 );
	}
    return true;
}
bool JagGeo::triangle3DDistanceEllipsoid(int srid,  double x10, double y10, double z10, double x20, double y20, double z20,
										double x30, double y30, double z30,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x10, y10, z10, x0, y0, z0, srid );
	double dist2 = JagGeo::distance( x20, y20, z20, x0, y0, z0, srid );
	double dist3 = JagGeo::distance( x30, y30, z30, x0, y0, z0, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax3( dist, dist2, dist3 );
	} else {
		dist = jagmin3( dist, dist2, dist3 );
	}
    return true;
}
bool JagGeo::triangle3DDistanceCone(int srid,  double x10, double y10, double z10, double x20, double y20, double z20,
										double x30, double y30, double z30,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x10, y10, z10, x0, y0, z0, srid );
	double dist2 = JagGeo::distance( x20, y20, z20, x0, y0, z0, srid );
	double dist3 = JagGeo::distance( x30, y30, z30, x0, y0, z0, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax3( dist, dist2, dist3 );
	} else {
		dist = jagmin3( dist, dist2, dist3 );
	}
    return true;
}



// 3D box
bool JagGeo::boxDistanceCube(int srid,  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0,
                            double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}
bool JagGeo::boxDistanceBox(int srid,  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
					       double x0, double y0, double z0, double w, double d, double h, double nx, double ny, 
						   const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}
bool JagGeo::boxDistanceSphere(int srid, double px0, double py0, double pz0, double a0, double b0, double c0,
                                 double nx0, double ny0, double x, double y, double z, double r,
							 const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x, y, z, px0, py0, pz0, srid );
    return true;
}

bool JagGeo::boxDistanceEllipsoid(int srid,  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
                                    double x0, double y0, double z0, double w, double d, double h, 
								double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}
    bool JagGeo::boxDistanceCone(int srid,  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
                                    double x0, double y0, double z0, double r, double h, 
								double nx, double ny, const AbaxDataString& arg, double &dist )
{
    return true;
}

// ellipsoid
bool JagGeo::ellipsoidDistanceCube(int srid,  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0,
                            double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}
bool JagGeo::ellipsoidDistanceBox(int srid,  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
					       double x0, double y0, double z0, double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}
bool JagGeo::ellipsoidDistanceSphere(int srid, double px0, double py0, double pz0, double a0, double b0, double c0,
                                 double nx0, double ny0, double x0, double y0, double z0, double r,
							 const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}
bool JagGeo::ellipsoidDistanceEllipsoid(int srid,  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
                                    double x0, double y0, double z0, double w, double d, double h, 
								double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}

bool JagGeo::ellipsoidDistanceCone(int srid,  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
                                    double x0, double y0, double z0, double r, double h, 
								double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}

// 3D cyliner
bool JagGeo::cylinderDistanceCube(int srid,  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
	                               double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}
bool JagGeo::cylinderDistanceBox(int srid,  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
	                                double x0, double y0, double z0, double w, double d, double h, 
									double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}
bool JagGeo::cylinderDistanceSphere(int srid,  double px, double py, double pz, double pr0, double c0,  double nx0, double ny0,
									double x0, double y0, double z0, double r, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px, py, pz, srid );
    return true;
}
bool JagGeo::cylinderDistanceEllipsoid(int srid,  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
	                                    double x0, double y0, double z0, double w, double d, double h, 
										double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}
bool JagGeo::cylinderDistanceCone(int srid,  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
	                                    double x0, double y0, double z0, double r, double h, 
										double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}

// 3D cone
bool JagGeo::coneDistanceCube(int srid,  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
	                               double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}
bool JagGeo::coneDistanceCube_test(int srid,  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
	                               double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}
bool JagGeo::coneDistanceBox(int srid,  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
	                                double x0, double y0, double z0, double w, double d, double h, 
									double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}
bool JagGeo::coneDistanceSphere(int srid,  double px, double py, double pz, double pr0, double c0,  double nx0, double ny0,
									double x0, double y0, double z0, double r, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px, py, pz, srid );
    return true;
}
bool JagGeo::coneDistanceEllipsoid(int srid,  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
	                                    double x0, double y0, double z0, double w, double d, double h, 
										double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}
bool JagGeo::coneDistanceCone(int srid,  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
	                                    double x0, double y0, double z0, double r, double h, 
										double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, px0, py0, pz0, srid );
    return true;
}


// 2D line
bool JagGeo::lineDistanceTriangle(int srid, double x10, double y10, double x20, double y20, 
									 double x1, double y1, double x2, double y2, double x3, double y3,  const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x10, y10, x1, y1, srid );
	double dist2 = JagGeo::distance( x10, y10, x2, y2, srid );
	double dist3 = JagGeo::distance( x20, y20, x3, y3, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax3( dist, dist2, dist3 );
	} else {
		dist = jagmin3( dist, dist2, dist3 );
	}
    return true;
}

bool JagGeo::lineDistanceLineString(int srid, double x10, double y10, double x20, double y20, 
	                              const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist )
{
	if ( sp2.length() < 3 ) { dist = 0.0; return false; }
	const char *str = sp2[2].c_str();
	char *p;
	double dx, dy;
	get2double(str, p, ':', dx, dy );
	dist = JagGeo::distance( dx, dy, x10, y10, srid );
    return true;
}

bool JagGeo::lineDistanceSquare(int srid, double x10, double y10, double x20, double y20, 
	                                double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, x10, y10, srid );
	double dist2 = JagGeo::distance( x0, y0, x20, y20, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax( dist, dist2 );
	} else {
		dist = jagmin( dist, dist2 );
	}
    return true;
}

bool JagGeo::lineDistanceRectangle(int srid, double x10, double y10, double x20, double y20, 
	                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, x10, y10, srid );
	double dist2 = JagGeo::distance( x0, y0, x20, y20, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax( dist, dist2 );
	} else {
		dist = jagmin( dist, dist2 );
	}
    return true;
}

bool JagGeo::lineDistanceEllipse(int srid, double x10, double y10, double x20, double y20, 
	                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, x10, y10, srid );
	double dist2 = JagGeo::distance( x0, y0, x20, y20, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax( dist, dist2 );
	} else {
		dist = jagmin( dist, dist2 );
	}
    return true;
}

bool JagGeo::lineDistanceCircle(int srid, double x10, double y10, double x20, double y20, 
								    double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, x10, y10, srid );
	double dist2 = JagGeo::distance( x0, y0, x20, y20, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax( dist, dist2 );
	} else {
		dist = jagmin( dist, dist2 );
	}
    return true;
}

bool JagGeo::lineDistancePolygon(int srid, double x10, double y10, double x20, double y20, 
								const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist )
{
	if ( sp2.length() < 3 ) { dist = 0.0; return false; }
	const char *str = sp2[2].c_str();
	char *p;
	double dx, dy;
	get2double(str, p, ':', dx, dy );
	dist = JagGeo::distance( dx, dy, x10, y10, srid );
	double dist2 = JagGeo::distance( dx, dy, x20, y20, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax( dist, dist2 );
	} else {
		dist = jagmin( dist, dist2 );
	}
    return true;
}


// line 3D
bool JagGeo::line3DDistanceLineString3D(int srid, double x10, double y10, double z10, double x20, double y20, double z20, 
	                              const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist )
{
	if ( sp2.length() < 3 ) { dist = 0.0; return false; }
	const char *str = sp2[2].c_str();
	char *p;
	double dx, dy, dz;
	get3double(str, p, ':', dx, dy, dz );
	dist = JagGeo::distance( dx, dy, dz, x10, y10, z10, srid );
	double dist2 = JagGeo::distance( dx, dy, dz, x20, y20, z20, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax( dist, dist2 );
	} else {
		dist = jagmin( dist, dist2 );
	}
    return true;
}

bool JagGeo::line3DDistanceCube(int srid,  double x10, double y10, double z10, double x20, double y20, double z20,
								double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, x10, y10, z10, srid );
	double dist2 = JagGeo::distance( x0, y0, z0, x20, y20, z20, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax( dist, dist2 );
	} else {
		dist = jagmin( dist, dist2 );
	}
    return true;
}

bool JagGeo::line3DDistanceBox(int srid,  double x10, double y10, double z10, double x20, double y20, double z20, 
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, x10, y10, z10, srid );
	double dist2 = JagGeo::distance( x0, y0, z0, x20, y20, z20, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax( dist, dist2 );
	} else {
		dist = jagmin( dist, dist2 );
	}
    return true;
}
bool JagGeo::line3DDistanceSphere(int srid,  double x10, double y10, double z10, double x20, double y20, double z20,
                                       double x0, double y0, double z0, double r, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, x10, y10, z10, srid );
	double dist2 = JagGeo::distance( x0, y0, z0, x20, y20, z20, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax( dist, dist2 );
	} else {
		dist = jagmin( dist, dist2 );
	}
    return true;
}
bool JagGeo::line3DDistanceEllipsoid(int srid,  double x10, double y10, double z10, double x20, double y20, double z20,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, x10, y10, z10, srid );
	double dist2 = JagGeo::distance( x0, y0, z0, x20, y20, z20, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax( dist, dist2 );
	} else {
		dist = jagmin( dist, dist2 );
	}
    return true;
}
bool JagGeo::line3DDistanceCone(int srid,  double x10, double y10, double z10, double x20, double y20, double z20,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	dist = JagGeo::distance( x0, y0, z0, x10, y10, z10, srid );
	double dist2 = JagGeo::distance( x0, y0, z0, x20, y20, z20, srid );
	if ( arg.caseEqual("max") ) { 
		dist = jagmax( dist, dist2 );
	} else {
		dist = jagmin( dist, dist2 );
	}
    return true;
}



// linestring 2d
bool JagGeo::lineStringDistanceLineString(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
										const AbaxDataString &mk2, const JagStrSplit &sp2,  const AbaxDataString& arg, double &dist )
{
    {
    	int start = 0;
    	if ( mk1 == JAG_OJAG ) {
    		start = 1;
    	}

        int start2 = 0;
        if ( mk2 == JAG_OJAG ) {
            start2 = 1;
        }

        double dx, dy, d, dx2, dy2, d2;
    	double mind = LONG_MAX;
    	double maxd = LONG_MIN;
        const char *str;
        char *p;

    	for ( int i=0; i < sp1.length(); ++i ) {
    		str = sp1[i].c_str();
    		if ( strchrnum( str, ':') < 1 || strchrnum( str, ':') > 2 ) continue;
    		get2double(str, p, ':', dx, dy );

    		for ( int i=0; i < sp2.length(); ++i ) {
            		str = sp2[i].c_str();
            		if ( strchrnum( str, ':') < 1 ) continue;
            		get2double(str, p, ':', dx2, dy2 );

                    d = JagGeo::distance( dx, dy, dx2, dy2, srid );
                    if ( d < mind ) mind = d;
                    if ( d > maxd ) maxd = d;
    	            }
        }
    	if ( arg.caseEqual( "max" ) ) {
    		dist = maxd;
    	} else {
    		dist = mind;
    	}

    	return true;
    }
	// todo002
	// sp1.print();
	// sp2.print();
//	dist = 0.0;
//    return true;
}


bool JagGeo::lineStringDistanceTriangle(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
									 double x1, double y1, double x2, double y2, double x3, double y3,  const AbaxDataString& arg, double &dist )
{
	int start = 0;
	if ( mk1 == JAG_OJAG ) {
		start = 1;
	}

    double dx, dy, min, max, d1, d2, d3;
	double mind = LONG_MAX;
	double maxd = LONG_MIN;
    const char *str;
    char *p;
	for ( int i=start; i < sp1.length(); ++i ) {
		str = sp1[i].c_str();
		if ( strchrnum( str, ':') < 1 ) continue;
		get2double(str, p, ':', dx, dy );
		d1 = JagGeo::distance( x1, y1, dx, dy, srid );
		d2 = JagGeo::distance( x2, y2, dx, dy, srid );
		d3 = JagGeo::distance( x3, y3, dx, dy, srid );
		min = jagmin3(d1,d2,d3);
		max = jagmax3(d1,d2,d3);
		if ( min < mind ) mind = min;
		if ( max > maxd ) maxd = max;
	}
	// todo003
	// sp1.print();
	// sp2.print();
    if ( arg.caseEqual( "max" ) ) {
        dist = maxd;
    } else {
        dist = mind;
    }

    return true;

}
bool JagGeo::lineStringDistanceSquare(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
	                                double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist )
{
	// todo004
	// sp1.print();
	// sp2.print();
	    int start = 0;
		if ( mk1 == JAG_OJAG ) {
    		start = 1;
    	}

        double dx, dy, d;
    	double mind = LONG_MAX;
    	double maxd = LONG_MIN;
        const char *str;
        char *p;
    	for ( int i=start; i < sp1.length(); ++i ) {
    		str = sp1[i].c_str();
    		if ( strchrnum( str, ':') < 1 ) continue;
    		get2double(str, p, ':', dx, dy );
    		d = JagGeo::distance( x0, y0, dx, dy, srid );
    		if ( d < mind ) mind = d;
    		if ( d > maxd ) maxd = d;
    	}

    	if ( arg.caseEqual( "max" ) ) {
    		dist = maxd + r;
    	} else {
    		dist = mind - r;
    	}

    	return true;
    }
//	dist = 0.0;
//    return true;
//}
bool JagGeo::lineStringDistanceRectangle(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
	                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist )
{
	// todo005
	// sp1.print();
	// sp2.print();

	int start = 0;
    if ( mk1 == JAG_OJAG ) {
        start = 1;
    }

    double dx, dy, d;
    double mind = LONG_MAX;
    double maxd = LONG_MIN;
    const char *str;
    char *p;
    for ( int i=start; i < sp1.length(); ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 1 ) continue;
        get2double(str, p, ':', dx, dy );
        d = JagGeo::distance( x0, y0, dx, dy, srid );
        if ( d < mind ) mind = d;
        if ( d > maxd ) maxd = d;
    }

    if ( arg.caseEqual( "max" ) ) {
        dist = maxd + jagmax(a,b);
    } else {
        dist = mind - jagmax(a,b);
    }

    return true;
}
bool JagGeo::lineStringDistanceEllipse(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
	                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist )
{
	// todo006 -- finish (need to solve binary quadratic equations for more percise result)
	// sp1.print();
	// sp2.print();
	//dist = 0.0;
	int start = 0;
	if ( mk1 == JAG_OJAG ) {
        start = 1;
    }

	double dx, dy, d;
	double mind = LONG_MAX;
    double maxd = LONG_MIN;
    double xsum = 0, ysum = 0;
    long counter = 0;
	const char *str;
    char *p;
	for (int i = start; i<sp1.length(); ++i){
		str = sp1[i].c_str();
		//printf("%s\n",str.c_str());
		if(strchrnum(str, ':') < 1) continue;
		get2double(str,p,':',dx,dy);
		double px, py;
        transform2DCoordGlobal2Local( dx, dy, x0, y0, nx, px, py ); //relocate ellipse center to origin point. transform (dx,dy) to (px,py)
		d = JagGeo::distance( x0, y0, px, py, srid ); //calculate each point on linestring to center of ellipse
		if ( d < mind ) mind = d;
        if ( d > maxd ) maxd = d;
        if  (arg.caseEqual("center" )) {
                            	xsum = xsum + dx;
                            	ysum = ysum + dy;
                            	counter ++;
                			}
	}

	if ( arg.caseEqual( "max" ) ) {
        dist = maxd + jagmax(a,b);
    } else if ( arg.caseEqual( "min" ) ){
        dist = mind - jagmax(a,b);
    } else if (arg.caseEqual("center")){
        if ( 0 == counter ) dist = 0.0;
        else dist = JagGeo::distance( xsum / counter, ysum / counter, x0, y0, srid );
    }
    return true;
}
bool JagGeo::lineStringDistanceCircle(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
								    double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist )
{
	// todo007 -- finish
	//srid -- 0: standard without unit, wgs24: global unit US standard
	// sp1.print();
	// sp2.print();
    //dist = 0.0;
    int start = 0;
	if ( mk1 == JAG_OJAG ) {
        start = 1;
        //JAG_OJAG: column name
        //JAG_CJAG: type in constant
    }
	double dx, dy, d;
	double mind = LONG_MAX;
    double maxd = LONG_MIN;
    double xsum = 0, ysum = 0;
    long counter = 0;
	const char *str;
    char *p;
	for (int i = start; i<sp1.length(); ++i){
		str = sp1[i].c_str();
		if(strchrnum(str, ':') < 1) continue;
		get2double(str,p,':',dx,dy);//get current (dx, dy), each pair is one point on the linestring
		d = JagGeo::distance( x0, y0, dx, dy, srid ); //calculate each point on linestring to center of circle
		if ( d < mind ) mind = d;
        if ( d > maxd ) maxd = d;
        if  (arg.caseEqual("center" )) {
                    	xsum = xsum + dx;
                    	ysum = ysum + dy;
                    	counter ++;
        			}
	}

	if ( arg.caseEqual( "max" ) ) {
        dist = maxd + r;
    } else if (arg.caseEqual( "min" )) {
        dist = mind - r;
    } else if  (arg.caseEqual("center" )){
    			if ( 0 == counter ) dist = 0.0;
    			else dist = JagGeo::distance( xsum / counter, ysum / counter, x0, y0, srid );
            }
    return true;
}
 	bool JagGeo::lineStringDistancePolygon(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
								     const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist )
{
	// todo008 -- finish
	sp1.print();
	sp2.print();
	prt(("s10001 JAG_OJAG sp2: prnt\n" ));
    JagPolygon pgon;
    int rc;
    int start = 0;
    int start2 = 0;
    double dx, dy, d;
    double mind = LONG_MAX;
    double maxd = LONG_MIN;
    const char *str;
    //const char *str2;
    char *p;
    char *p2;
    //prt(("s10001 colType2=[%s]\n", colType2.c_str() ));
    prt(("s10001 mk1=[%s]\n , mk2=[%s]\n", mk1.c_str(), mk2.c_str() ));
    if ( mk1 == JAG_OJAG ) {
        start = 1;
    }
    if ( mk2 == JAG_OJAG ) {
        start2 = 1;
        rc = JagParser::addPolygonData( pgon, sp2, true );
        //prt(("s10002 rc=%d false\n", rc ));
    } else {
        p2 = secondTokenStart( sp2.c_str() );
        //prt(("s10002 p2=[%s]\n", p2 ));
        rc = JagParser::addPolygonData( pgon, p2, true, false );
        //prt(("s10002 rc=%d false\n", rc ));
    }


    if ( rc < 0 ) {
            return false;
    }
    pgon.print();

    for ( int i=start; i < sp1.length(); ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 1 ) continue;
        get2double(str, p, ':', dx, dy );
        const JagLineString3D &linestr = pgon.linestr[0];
        for ( int i=0; i < linestr.size()-1; ++i ) {//first point = last point,so pass
            d = JagGeo::distance( dx, dy, linestr.point[i].x, linestr.point[i].y, srid );
            // prt(("s10003 d=[%f]\n",d));
            if ( d > maxd ) maxd = d;
            if ( d < mind ) mind = d;
        }
    }
    //prt(("s10005 xsum=[%f] ysum=[%f] xsum2=[%f] ysum2=[%f]\n",  xsum, ysum, xsum2, ysum2 ));

    if ( arg.caseEqual( "max" ) ) {
        dist = maxd;
    } else if (arg.caseEqual( "min" )) {
        dist = mind;
    } else if (arg.caseEqual( "center" )) {
        double px, py;
        lineStringAverage(mk1, sp1, px, py);
        double cx, cy;
        pgon.center2D(cx, cy);
        //prt(("s10006 px=[%f] py=[%f] cx=[%f] cy=[%f]", px, py, cx, cy));
        dist = JagGeo::distance( px, py, cx, cy, srid );
    }

    return true;
}

// linestring3d
bool JagGeo::lineString3DDistanceLineString3D(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
										    const AbaxDataString &mk2, const JagStrSplit &sp2,  const AbaxDataString& arg, double &dist )
{
	// todo009 -- finish
	// sp1.print();
	// sp2.print();
	//dist = 0.0;
    int start = 0;
    if ( mk1 == JAG_OJAG ) {
        start = 1;
    }

    int start2 = 0;
    if ( mk2 == JAG_OJAG ) {
        start2 = 1;
    }

    double dx, dy, dz, d, dx2, dy2, dz2, d2;
    double mind = LONG_MAX;
    double maxd = LONG_MIN;
    double xsum = 0, ysum = 0, zsum = 0, xsum2 = 0, ysum2 = 0, zsum2 = 0;
    long counter = 0;
    const char *str;
    const char *str2;
    char *p;
    char *p2;

    for ( int i=start; i < sp1.length(); ++i ) {
        str = sp1[i].c_str();
        if ( strchrnum( str, ':') < 2  ) continue;
        get3double(str, p, ':', dx, dy, dz );

        for ( int i=start2; i < sp2.length(); ++i ) {
                str2 = sp2[i].c_str();
                if ( strchrnum( str2, ':') < 1 ) continue;
                get3double(str2, p2, ':', dx2, dy2, dz2 );

                d = JagGeo::distance( dx, dy, dz, dx2, dy2, dz2, srid );
                if ( d < mind ) mind = d;
                if ( d > maxd ) maxd = d;
                if  (arg.caseEqual("center" )) {
                    xsum = xsum + dx;
                    ysum = ysum + dy;
                    zsum = zsum + dz;
                    xsum2 = xsum2 + dx2;
                    ysum2 = ysum2 + dy2;
                    zsum2 = zsum2 + dz2;
                    counter ++;
                }
        }
    }
    if (arg.caseEqual( "max" )) {
        dist = maxd;
    } else if (arg.caseEqual( "min" ))  {
        dist = mind;
    } else if (arg.caseEqual( "center" )){
        if ( 0 == counter ) dist = 0.0;
        else dist = JagGeo::distance( xsum / counter, ysum / counter, zsum/counter, xsum2 / counter, ysum2 / counter, zsum2/counter, srid );
    }

    return true;
}
bool JagGeo::lineString3DDistanceCube(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
								double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist )
{
    // todo010  -- finish
    // sp1.print(); //sp1: lineString3D
    // sp2.print(); //sp2: Cube
    //use lineString3DDistanceBox() w = d = h = r
    lineString3DDistanceBox(srid, mk1, sp1, x0, y0, z0, r, r, r, nx, ny, arg, dist);
    return true;
}

double JagGeo::DistanceOfPointToLine(double x0 ,double y0 ,double z0 ,double x1 ,double y1 ,double z1 ,double x2 ,double y2 ,double z2)
{
    double ab = sqrt(pow((x1 - x2), 2.0) + pow((y1 - y2), 2.0) + pow((z1 - z2), 2.0));
    double as = sqrt(pow((x1 - x0), 2.0) + pow((y1 - y0), 2.0) + pow((z1 - z0), 2.0));
    double bs = sqrt(pow((x0 - x2), 2.0) + pow((y0 - y2), 2.0) + pow((z0 - z2), 2.0));
    double cos_A = (pow(as, 2.0) + pow(ab, 2.0) - pow(bs, 2.0)) / (2 * ab*as);
    double sin_A = sqrt(1 - pow(cos_A, 2.0));
    return as*sin_A;
}

bool JagGeo::lineString3DDistanceBox(int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
        int start = 0;
        if ( mk1 == JAG_OJAG ) {
            start = 1;
        }

        double dx, dy, dz, pd, d1, px, py, pz, maxd1, maxd2, maxd3, mind1;
        double xsum = 0, ysum = 0, zsum = 0;
        long counter = 0;
        double mind = LONG_MAX;
        double maxd = LONG_MIN;
        const char *str;
        char *p;



        for ( int i=start; i < sp1.length(); ++i ) {
            str = sp1[i].c_str();
            if ( strchrnum( str, ':') < 1 ) continue;
            get3double(str, p, ':', dx, dy, dz );
            transform3DCoordGlobal2Local( x0, y0, z0, dx, dy, dz, nx, ny, px, py, pz );
            if (fabs(px) <= d && fabs(py) <= w){
                //point to up and down sides
                mind1 = fabs(fabs(pz) - h);
//                maxd2 = jagmax(distance( fabs(px), fabs(py), fabs(pz), -d, -w, -h, srid ), distance( fabs(px), fabs(py), fabs(pz), d, -w, -h, srid ));
//                maxd3 = jagmax(distance( fabs(px), fabs(py), fabs(pz), -d, w, -h, srid ), distance( fabs(px), fabs(py), fabs(pz), d, w, -h, srid ));
//                maxd1 = jagmax(maxd2, maxd3);
                maxd1 = distance( fabs(px), fabs(py), fabs(pz), -d, -w, -h, srid );
                if ( mind1 < mind ) mind = mind1;
                if ( maxd1 > maxd ) maxd = maxd1;
                prt(("1 min---%f\n", mind1));
                prt(("1 max---%f\n", maxd1));
                continue;
            }else if(fabs(py) <= w && fabs(pz) < h){
                //point to front and back sides
                mind1 = fabs(fabs(px) - d);
//                maxd2 = jagmax(distance( fabs(px), fabs(py), fabs(pz), -d, w, -h, srid ), distance( fabs(px), fabs(py), fabs(pz), -d, -w, -h, srid ));
//                maxd3 = jagmax(distance( fabs(px), fabs(py), fabs(pz), -d, w, h, srid ), distance( fabs(px), fabs(py), fabs(pz), -d, -w, h, srid ));
//                maxd1 = jagmax(maxd2, maxd3);
                maxd1 = distance( fabs(px), fabs(py), fabs(pz), -d, -w, -h, srid );
                if ( mind1 < mind ) mind = mind1;
                if ( maxd1 > maxd ) maxd = maxd1;
                prt(("2 min---%f\n", mind1));
                prt(("2 max---%f\n", maxd1));
                continue;
            }else if(fabs(px) <= d && fabs(pz) < h){
                //point to left and right sides
                mind1 = fabs(fabs(py) - w);
//                maxd2 = jagmax(distance( fabs(px), fabs(py), fabs(pz), -d, -w, -h, srid ), distance( fabs(px), fabs(py), fabs(pz), d, -w, -h, srid ));
//                maxd3 = jagmax(distance( fabs(px), fabs(py), fabs(pz), -d, -w, h, srid ), distance( fabs(px), fabs(py), fabs(pz), d, -w, h, srid ));
//                maxd1 = jagmax(maxd2, maxd3);
                maxd1 = distance( fabs(px), fabs(py), fabs(pz), -d, -w, -h, srid );
                if ( mind1 < mind ) mind = mind1;
                if ( maxd1 > maxd ) maxd = maxd1;
                prt(("3 min---%f\n", mind1));
                prt(("3 max---%f\n", maxd1));
                continue;
            }else if(fabs(px) <= d){
                //point to 4 depth lines
                mind1 = DistanceOfPointToLine(fabs(px), fabs(py), fabs(pz), d, w, h, -d, w, h);
//                maxd1 = jagmax(distance( fabs(px), fabs(py), fabs(pz), d, -w, -h, srid ), distance( fabs(px), fabs(py), fabs(pz), -d, -w, -h, srid ));
                maxd1 = distance( fabs(px), fabs(py), fabs(pz), -d, -w, -h, srid );
                if ( mind1 < mind ) mind = mind1;
                if ( maxd1 > maxd ) maxd = maxd1;
                prt(("4 min---%f\n", mind1));
                prt(("4 max---%f\n", maxd1));
                continue;
            }else if(fabs(py) <= w){
                //point to 4 width lines
                mind1 = DistanceOfPointToLine(fabs(px), fabs(py), fabs(pz), d, w, h, d, -w, h);
//                maxd1 = jagmax(distance( fabs(px), fabs(py), fabs(pz), -d, w, -h, srid ), distance( fabs(px), fabs(py), fabs(pz), -d, -w, -h, srid ));
                maxd1 = distance( fabs(px), fabs(py), fabs(pz), -d, -w, -h, srid );
                if ( mind1 < mind ) mind = mind1;
                if ( maxd1 > maxd ) maxd = maxd1;
                prt(("5 min---%f\n", mind1));
                prt(("5 max---%f\n", maxd1));
                continue;
            }else if(fabs(pz) <= h){
                //point to 4 height lines
                mind1 = DistanceOfPointToLine(fabs(px), fabs(py), fabs(pz), d, w, h, d, w, -h);
//                maxd1 = jagmax(distance( fabs(px), fabs(py), fabs(pz), -d, -w, h, srid ), distance( fabs(px), fabs(py), fabs(pz), -d, -w, -h, srid ));
                maxd1 = distance( fabs(px), fabs(py), fabs(pz), -d, -w, -h, srid );
                if ( mind1 < mind ) mind = mind1;
                if ( maxd1 > maxd ) maxd = maxd1;
                prt(("6 min---%f\n", mind1));
                prt(("6 max---%f\n", maxd1));
                continue;
            }else{
                //point to 8 points
                mind1 = distance( fabs(px), fabs(py), fabs(pz), d, w, h, srid );
                maxd1 = distance( fabs(px), fabs(py), fabs(pz), -d, -w, -h, srid );
                if ( mind1 < mind ) mind = mind1;
                if ( maxd1 > maxd ) maxd = maxd1;
                prt(("7 min---%f\n", mind1));
                prt(("7 max---%f\n", maxd1));
                continue;
            }
            if  (arg.caseEqual("center" )) {
                xsum = xsum + px;
                ysum = ysum + py;
                zsum = zsum + pz;
                counter ++;
            }
        }
        if ( arg.caseEqual( "max" ) ) {
            dist = maxd;
        } else if (arg.caseEqual("min" )){
            dist = mind;
        } else if  (arg.caseEqual("center" )){
			if ( 0 == counter ) dist = 0.0;
			else dist = JagGeo::distance( xsum / counter, ysum / counter, zsum / counter, 0, 0, 0, srid );
        }

	// todo011
	// sp1.print();
	// sp2.print();
//	dist = 0.0;
    return true;
}
bool JagGeo::lineString3DDistanceSphere(int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                       double x, double y, double z, double r, const AbaxDataString& arg, double &dist )
{
        int start = 0;
        if ( mk1 == JAG_OJAG ) {
            start = 1;
        }

        double dx, dy, dz, d, d1;
        double xsum = 0, ysum = 0, zsum = 0;
        long counter = 0;
        double mind = LONG_MAX;
        double maxd = LONG_MIN;
        const char *str;
        char *p;

        for ( int i=start; i < sp1.length(); ++i ) {
            str = sp1[i].c_str();
            if ( strchrnum( str, ':') < 1 ) continue;
            get3double(str, p, ':', dx, dy, dz );
            d = JagGeo::distance( dx, dy, dz, x, y, z, srid );
            if ( d < mind ) mind = d;
            if ( d > maxd ) maxd = d;
            // printf("%f\n",maxd);
        	if  (arg.caseEqual("center" )) {
            	xsum = xsum + dx;
            	ysum = ysum + dy;
            	zsum = zsum + dz;
            	counter ++;
			}
        }

        if ( arg.caseEqual( "max" ) ) {
            dist = maxd + r;
        } else if (arg.caseEqual("min" )){
            dist = mind - r;
        } else if  (arg.caseEqual("center" )){
			if ( 0 == counter ) dist = 0.0;
			else dist = JagGeo::distance( xsum / counter, ysum / counter, zsum / counter, x, y, z, srid );
        }

    return true;
	// todo012
	// sp1.print();
	// sp2.print();
}
bool JagGeo::lineString3DDistanceEllipsoid(int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	// todo013
	// sp1.print();
	// sp2.print();
	dist = 0.0;
    return true;
}
bool JagGeo::lineString3DDistanceCone(int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	// todo014
	// sp1.print();
	// sp2.print();
	dist = 0.0;
    return true;
}


// polygon
bool JagGeo::polygonDistanceTriangle(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
									 double x1, double y1, double x2, double y2, double x3, double y3,  const AbaxDataString& arg, double &dist )
{
    JagPolygon pgon;

    const char *str;
    char *p1;
    int rc;
    int start = 0;
    double d, d1, d2, d3, mind, maxd;
    double min = LONG_MAX;
    double max = LONG_MIN;
    double xsum = 0, ysum = 0;
    long counter = 0;

    if ( mk1 == JAG_OJAG ) {
        start = 1;
        rc = JagParser::addPolygonData( pgon, sp1, true );
    } else {
        p1 = secondTokenStart( sp1.c_str() );
        rc = JagParser::addPolygonData( pgon, p1, true, false );
    }
    if ( rc < 0 ) {
            return false;
    }

    const JagLineString3D &linestr = pgon.linestr[0];
    for ( int i=0; i < linestr.size()-1; ++i ) {//first point = last point,so pass
        d1 = JagGeo::distance( x1, y1, linestr.point[i].x, linestr.point[i].y, srid );
        d2 = JagGeo::distance( x2, y2, linestr.point[i].x, linestr.point[i].y, srid );
        d3 = JagGeo::distance( x3, y3, linestr.point[i].x, linestr.point[i].y, srid );
        if ( arg.caseEqual( "max" ) ) {
            mind = jagmin3(d1,d2,d3);
            if (mind < min){
                min = mind;
            }
        }else if (arg.caseEqual( "min" )) {
            maxd = jagmax3(d1,d2,d3);
            if (maxd > max){
                max = maxd;
            }
        }else if (arg.caseEqual("center" )) {
            xsum = xsum + linestr.point[i].x;
            ysum = ysum + linestr.point[i].y;
            counter++;
        }
    }

    if ( arg.caseEqual( "max" ) ) {
        dist = max;
    } else if (arg.caseEqual( "min" )) {
        dist = min;
    } else if (arg.caseEqual( "center" )) {
        double px, py;
        if ( 0 == counter ) dist = 0.0;
        else dist = JagGeo::distance( (x1 + x2 + x3) / 3, (y1 + y2 +y3) / 3, xsum / counter, ysum / counter, srid );
    }

    // todo015
	// sp1.print();
	// sp2.print();
    // dist = 0.0;
    return true;
}
bool JagGeo::polygonDistanceSquare(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
	                                double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist )
{
	// todo016
	// sp1.print();
	// sp2.print();
    polygonDistanceRectangle(srid, mk1, sp1, x0, y0, r, r, nx, arg, dist);
	//dist = 0.0;
    return true;
}
bool JagGeo::polygonDistanceRectangle(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
	                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist )
{
	// todo017
	// sp1.print();
	// sp2.print();
	JagPolygon pgon;
    int rc;
    int start = 0;
    int start2 = 0;
    double dx, dy, d;
    double mind = LONG_MAX;
    double maxd = LONG_MIN;
    const char *str;
    //const char *str2;
    char *p;
    char *p2;
    if ( mk1 == JAG_OJAG ) {
            start = 1;
            rc = JagParser::addPolygonData( pgon, sp1, true );
    }
    else{
        p = secondTokenStart( sp1.c_str() );
        rc = JagParser::addPolygonData( pgon, p, true, false );
    }
    if ( rc < 0 ) {
            return false;
        }

    pgon.print();
    const JagLineString3D &linestr = pgon.linestr[0];
    for ( int i=0; i < linestr.size()-1; ++i ) {//first point = last point,so pass
        pointDistanceRectangle(srid,x0,y0,linestr.point[i].x, linestr.point[i].y, a, b, nx, arg, d);
        if ( d > maxd ) maxd = d;
        if ( d < mind ) mind = d;
    }

    if ( arg.caseEqual( "max" ) ) {
            dist = maxd;
        } else if (arg.caseEqual( "min" )) {
            dist = mind;
        } else if (arg.caseEqual( "center" )) {
            double cx, cy;
            pgon.center2D(cx, cy);
            dist = JagGeo::distance( cx, cy, x0, y0, srid );
        }
        printf("%d\f",dist);

//	dist = 0.0;
    return true;
}

bool JagGeo::polygonDistanceEllipse(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
	                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist )
{
	// todo018
	// sp1.print();
	// sp2.print();
    dist = 0.0;
    return true;

}
 	bool JagGeo::polygonDistanceCircle(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
								    double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist )
{
    JagPolygon pgon;
    int rc;
    int start = 0;
    //int start2 = 0;
    double dx, dy, d;
    double mind = LONG_MAX;
    double maxd = LONG_MIN;
    const char *str;
    //const char *str2;
    char *p;
    char *p2;
    if ( mk1 == JAG_OJAG ) {
            start = 1;
            rc = JagParser::addPolygonData( pgon, sp1, true );
    }
    else{
        p = secondTokenStart( sp1.c_str() );
        rc = JagParser::addPolygonData( pgon, p, true, false );
    }
    if ( rc < 0 ) {
            return false;
        }

//    pgon.print();
    const JagLineString3D &linestr = pgon.linestr[0];
    for ( int i=0; i < linestr.size()-1; ++i ) {//first point = last point,so pass
        pointDistanceCircle(srid,linestr.point[i].x, linestr.point[i].y, x0, y0, r, arg, d);
        if ( d > maxd ) maxd = d;
        if ( d < mind ) mind = d;
    }

    if ( arg.caseEqual( "max" ) ) {
            dist = maxd;
        } else if (arg.caseEqual( "min" )) {
            dist = mind;
        } else if (arg.caseEqual( "center" )) {
            double cx, cy;
            pgon.center2D(cx,cy);
            dist = JagGeo::distance( cx, cy, x0, y0, srid );
        }
        printf("%d\f",dist);

//	dist = 0.0;

    return true;
}
bool JagGeo::polygonDistancePolygon(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
									const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist )
{
	// todo019
	// sp1.print();
	// sp2.print();

    JagPolygon pgon1;
    JagPolygon pgon2;

    int rc1, rc2;
    int start = 0;
    int start2 = 0;
    double dx, dy, d;
    double mind = LONG_MAX;
    double maxd = LONG_MIN;
//    const char *str;
//    const char *str2;

    char *p1;
    char *p2;

    //prt(("s10001 mk1=[%s]\n , mk2=[%s]\n", mk1.c_str(), mk2.c_str() ));

    //Polygon1
    if ( mk1 == JAG_OJAG ) {
        start = 1;
        rc1 = JagParser::addPolygonData( pgon1, sp1, true );
    }
    else {
        p1 = secondTokenStart( sp1.c_str() );
        rc1 = JagParser::addPolygonData( pgon1, p1, true, false );
        }

    //Polygon2
    if ( mk2 == JAG_OJAG ) {
        start2 = 1;
        rc1 = JagParser::addPolygonData( pgon2, sp2, true );

    } else {
        p2 = secondTokenStart( sp2.c_str() );
        rc2 = JagParser::addPolygonData( pgon2, p2, true, false );
    }


    if ( rc1 < 0 || rc2 < 0) {
            return false;
    }

    const JagLineString3D &linestr1 = pgon1.linestr[0];
    const JagLineString3D &linestr2 = pgon2.linestr[0];


    for ( int i=0; i < linestr1.size()-1; ++i ) {
        for ( int i=0; i < linestr2.size()-1; ++i ) {
            d = JagGeo::distance( linestr1.point[i].x, linestr1.point[i].y, linestr2.point[i].x, linestr2.point[i].y, srid );
            if ( d > maxd ) maxd = d;
            if ( d < mind ) mind = d;
            prt(("s10008 d=[%f]\n",d));
        }
    }


    if ( arg.caseEqual( "max" ) ) {
        dist = maxd;
    } else if (arg.caseEqual( "min" )) {
        dist = mind;
    } else if (arg.caseEqual( "center" )) {
        double px, py, cx, cy;
        pgon1.center2D(px,py);
        pgon2.center2D(cx,cy);
        dist = JagGeo::distance( px, py, cx, cy, srid );
    }

    return true;

}

// polygon3d Distance
bool JagGeo::polygon3DDistanceCube(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
								double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	// todo020
	// sp1.print();
	// sp2.print();
	// dist = 0.0;
	polygon3DDistanceBox(srid, mk1, sp1, x0, y0, z0, r, r, r, nx, ny, arg, dist);
    return true;
}

bool JagGeo::polygon3DDistanceBox(int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	// todo021
    //	sp1.print();
	// sp2.print();
	// dist = 0.0;
    JagPolygon pgon;
    const char *str;
    char *p1;
    int rc;
    int start = 0;
    double d1, d2, d3, min, max;
    double xsum = 0, ysum = 0, zsum = 0;
    long counter = 0;
    double dist1;
    double mind = LONG_MAX;
    double maxd = LONG_MIN;

    if ( mk1 == JAG_OJAG ) {
        start = 1;
        rc = JagParser::addPolygon3DData( pgon, sp1, true );
    } else {
        p1 = secondTokenStart( sp1.c_str() );
        rc = JagParser::addPolygon3DData( pgon, p1, true, false );
    }
    if ( rc < 0 ) {
            return false;
    }

    const JagLineString3D &linestr1 = pgon.linestr[0];

    for ( int i=0; i < linestr1.size()-1; ++i ) {//first point = last point,so pass
        if ( arg.caseEqual( "max" ) ) {
            point3DDistanceBox( srid, linestr1.point[i].x, linestr1.point[i].y, linestr1.point[i].z, x0, y0, z0, w, d, h, nx, ny, arg, dist1 );
            if (dist1 > maxd){
                maxd = dist1;
         }
        }else if ( arg.caseEqual( "min" ) ) {
            point3DDistanceBox( srid, linestr1.point[i].x, linestr1.point[i].y, linestr1.point[i].z, x0, y0, z0, w, d, h, nx, ny, arg, dist1 );
            if (dist1 < mind){
                mind = dist1;
            }
        }else if ( arg.caseEqual("center" ) ) {
            xsum = xsum + linestr1.point[i].x;
            ysum = ysum + linestr1.point[i].y;
            zsum = zsum + linestr1.point[i].z;
            counter++;
        }
    }

    if ( arg.caseEqual( "max" ) ) {
        dist = maxd;
    } else if (arg.caseEqual( "min" )) {
        dist = mind;
    } else if (arg.caseEqual( "center" )) {
        if ( 0 == counter ) dist = 0.0;
        else dist = JagGeo::distance( x0, y0, z0, xsum / counter, ysum / counter, zsum / counter, srid );
    }

    return true;
}
bool JagGeo::polygon3DDistanceSphere(int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                       double x, double y, double z, double r, const AbaxDataString& arg, double &dist )
{
	// todo022
	// sp1.print();
	// sp2.print();
	// dist = 0.0;
	JagPolygon pgon;
    const char *str;
    char *p1;
    int rc;
    int start = 0;
    double d1, d2, d3, min, max;
    double xsum = 0, ysum = 0, zsum = 0;
    long counter = 0;
    double dist1;
    double mind = LONG_MAX;
    double maxd = LONG_MIN;

    if ( mk1 == JAG_OJAG ) {
        start = 1;
        rc = JagParser::addPolygon3DData( pgon, sp1, true );
    } else {
        p1 = secondTokenStart( sp1.c_str() );
        rc = JagParser::addPolygon3DData( pgon, p1, true, false );
    }
    if ( rc < 0 ) {
            return false;
    }

    const JagLineString3D &linestr1 = pgon.linestr[0];

    for ( int i=0; i < linestr1.size()-1; ++i ) {//first point = last point,so pass
        dist1 = JagGeo::distance( linestr1.point[i].x, linestr1.point[i].y, linestr1.point[i].z, x, y, z, srid );
        if ( arg.caseEqual( "max" ) ) {
            if (dist1 > maxd){
                maxd = dist1;
         }
        }else if ( arg.caseEqual( "min" ) ) {
            if (dist1 < mind){
                mind = dist1;
            }
        }else if ( arg.caseEqual("center" ) ) {
            xsum = xsum + linestr1.point[i].x;
            ysum = ysum + linestr1.point[i].y;
            zsum = zsum + linestr1.point[i].z;
            counter++;
        }
    }

    if ( arg.caseEqual( "max" ) ) {
        dist = maxd + r;
    } else if (arg.caseEqual( "min" )) {
        dist = mind - r;
    } else if (arg.caseEqual( "center" )) {
        if ( 0 == counter ) dist = 0.0;
        else dist = JagGeo::distance( x, y, z, xsum / counter, ysum / counter, zsum / counter, srid );
    }

    return true;
}
bool JagGeo::polygon3DDistanceEllipsoid(int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	// todo023
	// sp1.print();
	// sp2.print();
	dist = 0.0;
    return true;
}
bool JagGeo::polygon3DDistanceCone(int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	// todo024
	// sp1.print();
	// sp2.print();
    // dist = 0.0;
    JagPolygon pgon;
    const char *str;
    char *p1;
    int rc;
    int start = 0;
    double d1, d2, d3, min, max;
    double xsum = 0, ysum = 0, zsum = 0;
    long counter = 0;
    double dist1;
    double mind = LONG_MAX;
    double maxd = LONG_MIN;

    if ( mk1 == JAG_OJAG ) {
        start = 1;
        rc = JagParser::addPolygon3DData( pgon, sp1, true );
    } else {
        p1 = secondTokenStart( sp1.c_str() );
        rc = JagParser::addPolygon3DData( pgon, p1, true, false );
    }
    if ( rc < 0 ) {
            return false;
    }

    const JagLineString3D &linestr1 = pgon.linestr[0];

    for ( int i=0; i < linestr1.size()-1; ++i ) {//first point = last point,so pass
        if ( arg.caseEqual( "max" ) ) {
            point3DDistanceCone( srid, linestr1.point[i].x, linestr1.point[i].y, linestr1.point[i].z, x0, y0, z0, r, h, nx, ny, arg, dist1 );
            if (dist1 > maxd){
                maxd = dist1;
         }
        }else if ( arg.caseEqual( "min" ) ) {
            point3DDistanceCone( srid, linestr1.point[i].x, linestr1.point[i].y, linestr1.point[i].z, x0, y0, z0, r, h, nx, ny, arg, dist1 );
            if (dist1 < mind){
                mind = dist1;
            }
        }else if ( arg.caseEqual("center" ) ) {
            xsum = xsum + linestr1.point[i].x;
            ysum = ysum + linestr1.point[i].y;
            zsum = zsum + linestr1.point[i].z;
            counter++;
        }
    }

    if ( arg.caseEqual( "max" ) ) {
        dist = maxd;
    } else if (arg.caseEqual( "min" )) {
        dist = mind;
    } else if (arg.caseEqual( "center" )) {
        if ( 0 == counter ) dist = 0.0;
        else dist = JagGeo::distance( x0, y0, z0, xsum / counter, ysum / counter, zsum / counter, srid );
    }

    return true;
}


// multipolygon -- 2D
bool JagGeo::multiPolygonDistanceTriangle(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
									 double x1, double y1, double x2, double y2, double x3, double y3,  const AbaxDataString& arg, double &dist )
{
	// todo025
	// sp1.print();
	// sp2.print();
	 char *p1;
	 int rc;
	 JagVector<JagPolygon> pgvec;
	 int start = 0;
	 double d1, d2, d3, min, max;
     double mind = LONG_MAX;
     double maxd = LONG_MIN;

	if ( mk1 == JAG_OJAG ) {
        start = 1;
        rc = JagParser::addMultiPolygonData( pgvec, sp1, true , false );
    }
    else {
        p1 = secondTokenStart( sp1.c_str() );
        rc = JagParser::addMultiPolygonData( pgvec, p1, true, false, false );
        }

    if ( rc < 0 ) {
                return false;
        }

    int len = pgvec.size();
    prt(("s10008 len=[%d]\n", len));
    if ( len < 1 ) return true;
    for ( int i=0; i < len; ++i ) {
    	const JagLineString3D &linestr = pgvec[i].linestr[0];
    	pgvec[i].print();
    	for ( int j=0; i < linestr.size()-1; ++i ) {
    	    d1 = JagGeo::distance( linestr.point[j].x, linestr.point[j].y, x1, y1, srid );
    	    d2 = JagGeo::distance( linestr.point[j].x, linestr.point[j].y, x2, y2, srid );
    	    d3 = JagGeo::distance( linestr.point[j].x, linestr.point[j].y, x3, y3, srid );
    	    min = jagmin3(d1,d2,d3);
            max = jagmax3(d1,d2,d3);
            if ( min < mind ) mind = min;
            if ( max > maxd ) maxd = max;
            prt(("s10008 d1=[%f] d2=[%f] d3=[%f] min=[%f] max=[%f]\n",  d1, d2, d3, min, max ));
    	}
    }

    if ( arg.caseEqual( "max" ) ) {
        dist = maxd;
    } else if (arg.caseEqual( "min" )) {
        dist = mind;
    } else if (arg.caseEqual( "center" )) {
        double cx,cy;
        center2DMultiPolygon(pgvec, cx, cy);
        dist = JagGeo::distance( cx, cy, (x1 + x2 + x3) / 3, (y1 + y2 +y3) / 3, srid );
    }

    return true;
}

bool JagGeo::multiPolygonDistanceSquare(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
	                                double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist )
{
	// todo026
	// sp1.print();
	// sp2.print();
	dist = 0.0;
    return true;
}
bool JagGeo::multiPolygonDistanceRectangle(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
	                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist )
{
	// todo027
	// sp1.print();
	// sp2.print();
	dist = 0.0;
    return true;
}
bool JagGeo::multiPolygonDistanceEllipse(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
	                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist )
{
	// todo028
	// sp1.print();
	// sp2.print();
	dist = 0.0;
    return true;
}
bool JagGeo::multiPolygonDistanceCircle(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
								    double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist )
{
	// todo029
	// sp1.print();
	// sp2.print();
	dist = 0.0;
    return true;
}

bool JagGeo::multiPolygonDistancePolygon(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
									const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist )
{
	// todo030
	// sp1.print();
	// sp2.print();
	dist = 0.0;
    return true;
}

// multipolygon3d Distance
bool JagGeo::multiPolygon3DDistanceCube(int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
								double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	// todo031
	printf("----------------");
	sp1.print();
	printf("----------------");
	// sp2.print();
    // dist = 0.0;
    int rc;
    int start = 0;
    int rc1;
    int start1 = 0;
    char *p1;
    JagVector<JagPolygon> pgvec;
    if ( mk1 == JAG_OJAG ) {
       start = 1;
       rc1 = JagParser::addMultiPolygonData( pgvec, sp1, true , true );
    } else {
       p1 = secondTokenStart( sp1.c_str() );
       rc1 = JagParser::addMultiPolygonData( pgvec, p1, true, false, false );
    }

    int len = pgvec.size();
    if ( len < 1 ) return false;
    double x, y, z;
    printf("-------11111111111---------\n");
    prt(("len:%d\n",len));
    printf("-------11111111111---------");
    for ( int i=0; i < len; ++i ) {
    	printf("-------^^^^^^^^^^---------");
        pgvec[i].print();
        printf("-------^^^^^^^^^^---------");
    }


    return true;
}

bool JagGeo::multiPolygon3DDistanceBox(int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	// todo032
	// sp1.print();
	// sp2.print();
	dist = 0.0;
    return true;
}
bool JagGeo::multiPolygon3DDistanceSphere(int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                       double x, double y, double z, double r, const AbaxDataString& arg, double &dist )
{
	// todo033
	// sp1.print();
	// sp2.print();
	dist = 0.0;
    return true;
}
bool JagGeo::multiPolygon3DDistanceEllipsoid(int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	// todo034
	// sp1.print();
	// sp2.print();
	dist = 0.0;
    return true;
}
bool JagGeo::multiPolygon3DDistanceCone(int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, const AbaxDataString& arg, double &dist )
{
	// todo035
	// sp1.print();
	// sp2.print();
	dist = 0.0;
    return true;
}



bool JagGeo::pointDistanceNormalEllipse(int srid, double px, double py, double w, double h, const AbaxDataString& arg, double &dist )
{
	// todo036
	// sp1.print();
	// sp2.print();
	dist = 0.0;
    return true;
}
bool JagGeo::point3DDistanceNormalEllipsoid(int srid, double px, double py, double pz, 
										   double w, double d, double h, const AbaxDataString& arg, double &dist )
{
	// todo037
	// sp1.print();
	// sp2.print();
	dist = 0.0;
    return true;
}
bool JagGeo::point3DDistanceNormalCone(int srid, double px, double py, double pz, 
									 double r, double h, const AbaxDataString& arg, double &dist )
{
	// todo038
	// sp1.print();
	// sp2.print();
	if ( arg.caseEqual( "center" ) ) {
		dist = distance( px, py, pz, 0.0, 0.0, 0.0, srid );
		return true;
	}

	// z-p plane (p=sqrt(x^2+y^2))
	double  pr2 = px*px + py*py;
	double  pr = sqrt( pr2 );
	if ( arg.caseEqual( "min" ) ) {
		if ( pz >= h ) {
			dist = distance(px, py, pz, 0.0, 0.0, h, srid );
		} else if ( pz <= -h ) {
			double  dx = pr - 2.0*r;
			double  dy = pz + h;
			dist = sqrt( dx*dx + dy*dy );
		} else {
			double d = r*( 1.0 - pz/h );
			dist = fabs(d - pr) * h*h/(h*h+r*r);
		}
	} else if ( arg.caseEqual( "max" ) ) {
		if ( pz >= h ) {
			double  pr2 = px*px + py*py;
			double  pr = sqrt( pr2 );
			dist = distance(0.0, pr, pz,  0.0, -2.0*r, -h, srid );
		} else if ( pz <= -h ) {
			double  dx = pr + 2.0*r;
			double  dy = pz + h;
			dist = sqrt( dx*dx + dy*dy );
		} else {
			double d = r*( 1.0 - pz/h );
			dist = fabs(d + pr) * h*h/(h*h+r*r);
		}
	}
    return true;
}

double JagGeo::pointToLineDistance( double lata1, double lona1, double lata2, double lona2, double latb1, double lonb1 )
{
	double lat0 = (lata1 + lata2 )/2.0;
	double lon0 = (lona1 + lona2 )/2.0;

	const Geodesic& geod = Geodesic::WGS84();
  	const GeographicLib::Gnomonic gn(geod);

	double xa1, ya1, xa2, ya2;
	double xb1, yb1, xb2, yb2;
	double lat1, lon1;
	for (int i = 0; i < 10; ++i) {
		gn.Forward(lat0, lon0, lata1, lona1, xa1, ya1);
	    gn.Forward(lat0, lon0, lata2, lona2, xa2, ya2);
	    gn.Forward(lat0, lon0, latb1, lonb1, xb1, yb1);
	    vector3 va1(xa1, ya1); vector3 va2(xa2, ya2);
	    vector3 la = va1.cross(va2);
	    vector3 vb1(xb1, yb1);
	    vector3 lb(la._y, -la._x, la._x * yb1 - la._y * xb1);
	    vector3 p0 = la.cross(lb);
	    p0.norm();
	    gn.Reverse(lat0, lon0, p0._x, p0._y, lat1, lon1);
	    lat0 = lat1;
	    lon0 = lon1;
    }

	//prt(("s2938 close point lat0=%.3f lon0=%.3f\n", lat0, lon0 ));
	geod.Inverse( latb1, lonb1, lat0, lon0, xa1 );  // use xa1
	return xa1;
}

double JagGeo::meterToLon( int srid, double meter, double lon, double lat)
{
	if ( 0 == srid ) return meter;  // geometric unitless length

	const Geodesic& geod = Geodesic::WGS84();
	double lon2 = lon + 1.0;
	double s12;
	geod.Inverse( lat, lon, lat, lon2, s12 );
	return meter/s12;
}

double JagGeo::meterToLat( int srid, double meter, double lon, double lat)
{
	if ( 0 == srid ) return meter;  // geometric unitless length

	const Geodesic& geod = Geodesic::WGS84();
	double lat2 = lat + 1.0;
	double s12;
	geod.Inverse( lat, lon, lat2, lon, s12 );
	return meter/s12;
}

bool JagGeo::lineStringAverage( const AbaxDataString &mk, const JagStrSplit &sp, double &x, double &y )
{
	int start = 0;
	if ( mk == JAG_OJAG ) { start = 1; }

	double dx, dy;
    double xsum = 0.0, ysum = 0.0;
	long  counter = 0;
	const char *str;
    char *p;
	for (int i = start; i<sp.length(); ++i){
		str = sp[i].c_str();
		if(strchrnum(str, ':') < 1) continue;
		get2double(str,p,':',dx,dy);
        xsum = xsum + dx;
        ysum = ysum + dy;
        counter ++;
	}

    if ( 0 == counter ) { 
		x = y = 0.0;
		return false; 
	} else {
		x = xsum/(double)counter;
		y = ysum/(double)counter;
	}

	return true;
}


bool JagGeo::lineString3DAverage( const AbaxDataString &mk, const JagStrSplit &sp, double &x, double &y, double &z )
{
	int start = 0;
	if ( mk == JAG_OJAG ) { start = 1; }

	double dx, dy, dz;
    double xsum = 0.0, ysum = 0.0, zsum = 0.0;
	long  counter = 0;
	const char *str;
    char *p;
	for (int i = start; i<sp.length(); ++i){
		str = sp[i].c_str();
		if(strchrnum(str, ':') < 2) continue;
		get3double(str,p,':', dx,dy,dz);
        xsum = xsum + dx;
        ysum = ysum + dy;
        zsum = zsum + dz;
        counter ++;
	}

    if ( 0 == counter ) { 
		x = y = z = 0.0;
		return false; 
	} else {
		x = xsum/(double)counter;
		y = ysum/(double)counter;
		z = zsum/(double)counter;
	}

	return true;
}

void JagPolygon::center2D(  double &cx, double &cy ) const
{
	cx = cy = 0.0;
	// JagVector<JagLineString3D> linestr;
	int len = linestr.size();
	//prt(("s10007 len=[%d]\n", len ));
	if ( len < 1 ) return;
	double x, y;
	for ( int i=0; i < len; ++i ) {
		linestr[i].center2D(x, y, true);
		cx += x;
		cy += y;
	}

	cx = cx / len;
	cy = cy / len;
}

void JagPolygon::center3D(  double &cx, double &cy, double &cz ) const
{
	cx = cy = cz = 0.0;
	int len = linestr.size();
	if ( len < 1 ) return;
	double x, y, z;
	for ( int i=0; i < len; ++i ) {
		linestr[i].center3D(x, y, z, true);

		cx += x;
		cy += y;
		cz += z;
	}

	cx = cx / len;
	cy = cy / len;
	cz = cz / len;
}

void JagGeo::center2DMultiPolygon( const JagVector<JagPolygon> &pgvec, double &cx, double &cy )
{
	cx = cy = 0.0;
	int len = pgvec.size();
	if ( len < 1 ) return;
	double x, y;
	for ( int i=0; i < len; ++i ) {
		pgvec[i].center2D(x, y);
		cx += x;
		cy += y;
	}

	cx = cx / len;
	cy = cy / len;
}

void JagGeo::center3DMultiPolygon( const JagVector<JagPolygon> &pgvec, double &cx, double &cy, double &cz )
{
	cx = cy = cz = 0.0;
	int len = pgvec.size();
	if ( len < 1 ) return;
	double x, y, z;
	for ( int i=0; i < len; ++i ) {
		pgvec[i].center3D(x, y, z);
		cx += x;
		cy += y;
		cz += z;
	}

	cx = cx / len;
	cy = cy / len;
	cz = cz / len;
}

