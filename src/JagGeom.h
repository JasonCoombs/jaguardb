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
#ifndef _jag_geom_h_
#define _jag_geom_h_

#include <JagDef.h>
#include <JagVector.h>
#include <JagUtil.h>
//#include <vector>

#define jagmin(a, b) ( (a) < (b) ? (a) : (b))
#define jagmax(a, b) ( (a) > (b) ? (a) : (b))
#define jagmin3(a, b, c) jagmin(jagmin(a, b),(c))
#define jagmax3(a, b, c) jagmax(jagmax(a, b),(c))
#define jagsq2(a) ( (a)*(a) )
#define jagIsPosZero(a) (((a)<ZERO) ? 1 : 0)
#define jagIsZero(a) (( fabs(a)<ZERO) ? 1 : 0)
#define JAG_PI 3.141592653589793

class JagNameIndex;

class JagPoint2D
{
  public:
     JagPoint2D();
     JagPoint2D( const char *sx, const char *sy);
     JagPoint2D( double inx, double iny );
	 void transform( double x0, double y0, double nx0 );
	 void print() const { printf("x=%.1f y=%.1f ", x, y ); }
     double x;
	 double y;
};

class JagSortPoint2D
{
  public:
     double x1, y1;
     double x2, y2;
	 unsigned char    end;
	 unsigned char    color;
	 bool operator < ( const JagSortPoint2D &o) const;
	 bool operator <= ( const JagSortPoint2D &o) const;
	 bool operator > ( const JagSortPoint2D &o) const;
	 bool operator >= ( const JagSortPoint2D &o) const;
	 void print() const { printf("x1=%.1f y1=%.1f x2=%.1f y2=%.1f ", x1, y1, x2, y2 ); }
	 
};


class JagPoint3D
{
  public:
     JagPoint3D();
     JagPoint3D( const char *sx, const char *sy, const char *sz );
     JagPoint3D( double inx, double iny, double inz );
	 void transform( double x0, double y0, double z0, double nx0, double ny0 );
	 void print() const { printf("x=%.1f y=%.1f z=%.1f ", x, y, z ); }
     double x;
	 double y;
	 double z;
};

class JagSortPoint3D
{
  public:
     double x1, y1, z1;
     double x2, y2, z2;
	 unsigned char   end;
	 unsigned char   color;
	 bool operator < ( const JagSortPoint3D &o) const;
	 bool operator <= ( const JagSortPoint3D &o) const;
	 bool operator > ( const JagSortPoint3D &o) const;
	 bool operator >= ( const JagSortPoint3D &o) const;
	 void print() const { printf("x1=%.1f y1=%.1f z1=%.1f x2=%.1f y2=%.1f z2=%.1f ", x1, y1, z1, x2, y2, z2 ); }
};

class JagLine2D
{
  public:
  	 JagLine2D() { };
  	 JagLine2D( double ix1, double iy1, double ix2, double iy2 )
	 { x1 = ix1; y1 = iy1; x2 = ix2; y2 = iy2; }
	 void transform( double x0, double y0, double nx0 );
	 void center(double &cx, double &cy ) const { cx=(x1+x2)/2.0; cy=(y1+y2)/2.0; }
     double x1;
	 double y1;
     double x2;
	 double y2;
};

class JagLine3D
{
  public:
  	 JagLine3D() { };
  	 JagLine3D( double ix1, double iy1, double iz1, double ix2, double iy2, double iz2 )
	 { x1 = ix1; y1 = iy1; z1 = iz1; x2 = ix2; y2 = iy2; z2 = iz2; }
     double x1;
	 double y1;
	 double z1;
     double x2;
	 double y2;
	 double z2;
	 void transform( double x0, double y0, double z0, double nx0, double ny0 );
	 void center( double &cx, double &cy, double &cz ) const { cx=(x1+x2)/2.0; cy=(y1+y2)/2.0; cz=(z1+z2)/2.0; }

};

class JagLineSeg2D
{
  public:
    JagLineSeg2D() {}
    JagLineSeg2D( double f1, double f2, double f3, double f4 ) { 
		x1=f1; y1=f2; x2=f3; y2=f4; }
	bool operator > ( const JagLineSeg2D &o) const;
	bool operator < ( const JagLineSeg2D &o) const;
	bool operator >= ( const JagLineSeg2D &o) const;
	bool operator <= ( const JagLineSeg2D &o) const;
	bool operator == ( const JagLineSeg2D &o) const;
	bool operator != ( const JagLineSeg2D &o) const;
	abaxint hashCode() const ;
    double x1;
	double y1;
    double x2;
	double y2;
	unsigned char color;
	abaxint size() const { return 32; }
	static JagLineSeg2D NULLVALUE;
	void println() const { printf("JagLineSeg2D: x1=%.3f y1=%.3f x2=%.3f y2=%.3f\n", x1, y1, x2, y2 ); }
	void print() const { printf("JagLineSeg2D: x1=%.3f y1=%.3f x2=%.3f y2=%.3f ", x1, y1, x2, y2 ); }
	bool  isNull() const;
};

class JagLineSeg3D
{
  public:
    JagLineSeg3D() {}
    JagLineSeg3D( double f1, double f2, double f3, double f4, double f5, double f6 ) { 
		x1=f1; y1=f2; z1=f3; x2=f4; y2=f5; z2=f6; }
	bool operator > ( const JagLineSeg3D &o) const;
	bool operator < ( const JagLineSeg3D &o) const;
	bool operator >= ( const JagLineSeg3D &o) const;
	bool operator <= ( const JagLineSeg3D &o) const;
	bool operator == ( const JagLineSeg3D &o) const;
	bool operator != ( const JagLineSeg3D &o) const;
	abaxint hashCode() const ;
    double x1;
	double y1;
	double z1;
    double x2;
	double y2;
	double z2;
	unsigned char color;
	abaxint size() const { return 64; }
	static JagLineSeg3D NULLVALUE;
	void println() const { printf("JagLineSeg3D: x1=%.1f y1=%.1f z1=%.1f x2=%.1f y2=%.1f z2=%.1f\n", x1, y1, z1, x2, y2, z2 ); }
	void print() const { printf("JagLineSeg3D: x1=%.1f y1=%.1f z1=%.1f x2=%.1f y2=%.1f z2=%.1f ", x1, y1, z1, x2, y2, z2 ); }
	bool  isNull() const;
};



class JagLineSeg2DPair
{
    public:
		JagLineSeg2DPair() {}
        JagLineSeg2DPair( const JagLineSeg2D &k ) : key(k) {}
        JagLineSeg2D  key;
        bool value;
		unsigned char color;
		static  JagLineSeg2DPair  NULLVALUE;

		// operators
        inline int operator< ( const JagLineSeg2DPair &d2 ) const {
    		return (key < d2.key);
		}
        inline int operator<= ( const JagLineSeg2DPair &d2 ) const {
    		return (key <= d2.key );
		}
		
        inline int operator> ( const JagLineSeg2DPair &d2 ) const {
    		return (key > d2.key );
		}

        inline int operator >= ( const JagLineSeg2DPair &d2 ) const
        {
    		return (key >= d2.key );
        }
        inline int operator== ( const JagLineSeg2DPair &d2 ) const
        {
    		return ( key == d2.key );
        }
        inline int operator!= ( const JagLineSeg2DPair &d2 ) const
        {
    		return ( key != d2.key );
        }

		inline void valueDestroy( AbaxDestroyAction action ) { }

		inline abaxint hashCode() const {
			return key.hashCode();
		}

		inline abaxint size() { return 32; }
		void print() const { key.print(); }
		void println() const { key.println(); }
};

class JagLineSeg3DPair
{
    public:
		JagLineSeg3DPair() {}
        JagLineSeg3DPair( const JagLineSeg3D &k ) : key(k) {}
        JagLineSeg3D  key;
        bool value;
		unsigned char color;
		static  JagLineSeg3DPair  NULLVALUE;

		// operators
        inline int operator< ( const JagLineSeg3DPair &d2 ) const {
    		return (key < d2.key);
		}
        inline int operator<= ( const JagLineSeg3DPair &d2 ) const {
    		return (key <= d2.key );
		}
		
        inline int operator> ( const JagLineSeg3DPair &d2 ) const {
    		return (key > d2.key );
		}

        inline int operator >= ( const JagLineSeg3DPair &d2 ) const
        {
    		return (key >= d2.key );
        }
        inline int operator== ( const JagLineSeg3DPair &d2 ) const
        {
    		return ( key == d2.key );
        }
        inline int operator!= ( const JagLineSeg3DPair &d2 ) const
        {
    		return ( key != d2.key );
        }

		inline void valueDestroy( AbaxDestroyAction action ) { }
		inline abaxint hashCode() const {
			return key.hashCode();
		}

		inline abaxint size() { return 64; }
		void print() const { key.print(); }
		void println() const { key.println(); }
};


class JagRectangle3D
{
  public:
    JagRectangle3D() { nx=ny=0.0f; }
  	double x, y, z, w, h, nx, ny;
	void transform( double x0, double y0, double z0, double nx0, double ny0 );
};

class JagTriangle3D
{
  public:
  	double x1, y1, z1, x2,y2,z2, x3,y3,z3;
	void transform( double x0, double y0, double z0, double nx0, double ny0 );
};


class JagPoint
{
	public:
		JagPoint();
		JagPoint( const char *x, const char *y );
		JagPoint( const char *x, const char *y, const char *z );
		JagPoint& operator=( const JagPoint& p2 );
		JagPoint( const JagPoint& p2 );
		void init();
		void copyData( const JagPoint& p2 );

		char x[JAG_POINT_LEN]; // or longitue
		char y[JAG_POINT_LEN]; // or latitude
		char z[JAG_POINT_LEN]; // or azimuth, altitude

		char a[JAG_POINT_LEN];  // width
		char b[JAG_POINT_LEN];  // depth in a box
		char c[JAG_POINT_LEN];  // height

		char nx[JAG_POINT_LEN];  // normalized basis vector in x
		char ny[JAG_POINT_LEN];  // normalized basis vector in y
		void print() const;
};

class JagLineString3D
{
    public:
		JagLineString3D() {};
		JagLineString3D& operator=( const JagLineString3D& L2 ) { point = L2.point; return *this; }
		JagLineString3D( const JagLineString3D& L2 ) { point = L2.point; }
		void init() { point.clean(); };
		abaxint size() const { return point.size(); }
		void add( const JagPoint2D &p );
		void add( const JagPoint3D &p );
		void add( double x, double y, double z );
		void print() const { point.print(); }
		void center2D( double &cx, double &cy ) const;
		void center3D( double &cx, double &cy, double &cz ) const;

		JagVector<JagPoint3D> point;
};

class JagLineString
{
    public:
		JagLineString() {};
		JagLineString& operator=( const JagLineString& L2 ) { point = L2.point; return *this; }
		JagLineString& operator=( const JagLineString3D& L2 );
		JagLineString( const JagLineString& L2 ) { point = L2.point; }
		void init() { point.clean(); };
		abaxint size() const { return point.size(); }
		void add( const JagPoint2D &p );
		void add( const JagPoint3D &p );
		void add( double x, double y );
		void print() const { point.print(); }
		void center2D( double &cx, double &cy ) const;
		void center3D( double &cx, double &cy, double &cz ) const;

		JagVector<JagPoint> point;
};


class JagPolygon
{
	public:
		JagPolygon() {};
		JagPolygon( const JagPolygon &p2 ) { linestr = p2.linestr; }
		JagPolygon& operator=( const JagPolygon &p2 ) { linestr = p2.linestr; return *this; }
		void init() { linestr.clean(); }
		abaxint size() const { return linestr.size(); }
		void add( const JagLineString3D &linestr3d ) { linestr.append(linestr3d); }
		void center2D( double &cx, double &cy ) const;
		void center3D( double &cx, double &cy, double &cz ) const;
		JagVector<JagLineString3D> linestr;
		void print() const { linestr.print(); }
		
};


class vector3 
{
  public:
  double _x, _y, _z;
  vector3(double x, double y, double z = 1) throw()
    : _x(x)
    , _y(y)
    , _z(z) {}

  vector3 cross(const vector3& b) const throw() {
    return vector3(_y * b._z - _z * b._y,
                   _z * b._x - _x * b._z,
                   _x * b._y - _y * b._x);
  }

  void norm() throw() {
    _x /= _z;
    _y /= _z;
    _z = 1;
  }
};





// qwer
class JagGeo
{
  public:

	/////////////////////////////////////// Within function ////////////////////////////////////////////////////
   	static bool doPointWithin(  const JagStrSplit &sp1, const AbaxDataString &mk2, 
								const AbaxDataString &colType2, int srid2, 
								const JagStrSplit &sp2, bool strict=true );
   	static bool doPoint3DWithin( const JagStrSplit &sp1, const AbaxDataString &mk2, 
								const AbaxDataString &colType2, int srid2, 
								const JagStrSplit &sp2, bool strict=true );
   	static bool doTriangleWithin(  int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, int srid2, 
									const JagStrSplit &sp2,  bool strict=true );
   	static bool doTriangle3DWithin(  int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, int srid2, 
									const JagStrSplit &sp2,  bool strict=true );
   	static bool doCircleWithin(  int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, int srid2, 
									const JagStrSplit &sp2,  bool strict=true );
   	static bool doCircle3DWithin(  int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, int srid2, 
									const JagStrSplit &sp2,  bool strict=true );
   	static bool doSphereWithin(  int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doBoxWithin(  int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doRectangleWithin(  int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doRectangle3DWithin(  int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doSquareWithin(  int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doSquare3DWithin(  int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									  int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doCubeWithin(  int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									 int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doCylinderWithin(  int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									 int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doConeWithin(  int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doEllipseWithin(  int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doEllipsoidWithin(  int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doLineWithin(  int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doLine3DWithin(  int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doLineStringWithin(  const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, 
									const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doLineString3DWithin( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, 
									  const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doPolygonWithin(  const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, 
									const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doPolygon3DWithin( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, 
									  const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doMultiPolygonWithin(  const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, 
									const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doMultiPolygon3DWithin(  const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, 
									const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, bool strict=true );


	// 2D point
	static bool pointWithinPoint( double px, double py, double x1, double y1, bool strict );
	static bool pointWithinLine( double px, double py, double x1, double y1, double x2, double y2, bool strict );
	static bool pointWithinLineString(  double x, double y, const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict );
	//static bool pointWithinSquare( double px, double py, double x0, double y0, double r, double nx, bool strict );
	static bool pointWithinCircle( double px, double py, double x0, double y0, double r, bool strict );
	static bool pointWithinRectangle( double px, double py, double x, double y, double a, double b, double nx, bool strict );
	static bool pointWithinEllipse( double px, double py, double x, double y, double a, double b, double nx, bool strict );
	static bool pointWithinTriangle( const JagPoint2D &point, 
									  const JagPoint2D &point1, const JagPoint2D &point2,
					 				  const JagPoint2D &point3, bool strict = false, bool boundcheck = true );
	static bool pointInTriangle( double px, double py, double x1, double y1,
									  double x2, double y2, double x3, double y3,
					 				  bool strict = false, bool boundcheck = true );
	static bool pointWithinPolygon( double x, double y, const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict );
	static bool pointWithinPolygon( double x, double y, const JagLineString3D &linestr );
	static bool pointWithinPolygon( double x, double y, const JagPolygon &pgon );

	// 3D point
	static bool point3DWithinPoint3D( double px, double py, double pz, double x1, double y1, double z1, bool strict );
	static bool point3DWithinLine3D( double px, double py, double pz, double x1, double y1, double z1, 
									double x2, double y2, double z2, bool strict );
	static bool point3DWithinLineString3D(  double x, double y, double z, const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict );
	static bool point3DWithinBox( double px, double py, double pz,  
									  double x, double y, double z, double a, double b, double c, double nx, double ny, 
									  bool strict );
    static bool point3DWithinSphere( double px, double py, double pz, double x, double y, double z, double r, bool strict );
	static bool point3DWithinEllipsoid( double px, double py, double pz,  
									  double x, double y, double z, double a, double b, double c, double nx, double ny, 
									  bool strict );

	static bool point3DWithinCone( double px, double py, double pz, 
									double x0, double y0, double z0,
									 double r, double h,  double nx, double ny, bool strict );
	static bool point3DWithinSquare3D( double px, double py, double pz, 
									double x0, double y0, double z0,
									 double a, double nx, double ny, bool strict );
	static bool point3DWithinCylinder(  double x, double y, double z,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, bool strict );
	static bool point3DWithinNormalCylinder(  double x, double y, double z,
                                    double r, double h, bool strict );

	// 2D circle
	static bool circleWithinCircle( double px, double py, double pr, double x, double y, double r, bool strict );
	static bool circleWithinSquare( double px0, double py0, double pr, double x0, double y0, double r, double nx, bool strict );
	static bool circleWithinEllipse( double px, double py, double pr, 
									 	 double x, double y, double w, double h, double nx, 
									 	 bool strict=true, bool bound=true );
	static bool circleWithinRectangle( double px0, double py0, double pr, double x0, double y0,
										  double w, double h, double nx,  bool strict );
	static bool circleWithinTriangle( double px, double py, double pr, double x1, double y1, 
									  double x2, double y2, double x3, double y3, bool strict=true, bool bound = true );
	static bool circleWithinPolygon( double px0, double py0, double pr, 
									 const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict );

	// 3D circle
	static bool circle3DWithinCube( double px0, double py0, double pz0, double pr0,  double nx0, double ny0,
										double x0, double y0, double z0,  double r, double nx, double ny, bool strict );

	static bool circle3DWithinBox( double px0, double py0, double pz0, double pr0,  double nx0, double ny0,
					                   double x0, double y0, double z0,  double a, double b, double c,
					                   double nx, double ny, bool strict );
   	static bool circle3DWithinSphere( double px0, double py0, double pz0, double pr0,   double nx0, double ny0,
	   									 double x, double y, double z, double r, bool strict );
	static bool circle3DWithinEllipsoid( double px0, double py0, double pz0, double pr0,  double nx0, double ny0,
										  double x0, double y0, double z0, 
									 	   double w, double d, double h, double nx, double ny, bool strict );
	static bool circle3DWithinCone( double px0, double py0, double pz0, double pr0, double nx0, double ny0,
										  double x0, double y0, double z0, 
									 	   double r, double h, double nx, double ny, bool strict );

	// 3D sphere
	static bool sphereWithinCube(  double px0, double py0, double pz0, double pr0,
		                               double x0, double y0, double z0, double r, double nx, double ny, bool strict );
	static bool sphereWithinBox(  double px0, double py0, double pz0, double r,
		                                double x0, double y0, double z0, double w, double d, double h, 
										double nx, double ny, bool strict );
	static bool sphereWithinSphere(  double px, double py, double pz, double pr, 
										double x, double y, double z, double r, bool strict );
	static bool sphereWithinEllipsoid(  double px0, double py0, double pz0, double pr,
		                                    double x0, double y0, double z0, double w, double d, double h, 
											double nx, double ny, bool strict );
	static bool sphereWithinCone(  double px0, double py0, double pz0, double pr,
		                                    double x0, double y0, double z0, double r, double h, 
											double nx, double ny, bool strict );

	// 2D rectangle
	static bool rectangleWithinTriangle( double px0, double py0, double a0, double b0, double nx0, 
										 double x1, double y1, double x2, double y2, double x3, double y3, bool strict );
	static bool rectangleWithinSquare( double px0, double py0, double a0, double b0, double nx0,
		                                double x0, double y0, double r, double nx, bool strict );
	static bool rectangleWithinRectangle( double px0, double py0, double a0, double b0, double nx0,
		                                double x0, double y0, double a, double b, double nx, bool strict );
	static bool rectangleWithinEllipse( double px0, double py0, double a0, double b0, double nx0,
		                                double x0, double y0, double a, double b, double nx, bool strict );
 	static bool rectangleWithinCircle( double px0, double py0, double a0, double b0, double nx0, 
									    double x0, double y0, double r, double nx, bool strict );
 	static bool rectangleWithinPolygon( double px0, double py0, double a0, double b0, double nx0, 
										const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict );

	// 2D triangle
	static bool triangleWithinTriangle( double x10, double y10, double x20, double y20, double x30, double y30,
										 double x1, double y1, double x2, double y2, double x3, double y3, bool strict );
	 /***
	static bool triangleWithinSquare( double x10, double y10, double x20, double y20, double x30, double y30,
		                                double x0, double y0, double r, double nx, bool strict );
	***/
	static bool triangleWithinRectangle( double x10, double y10, double x20, double y20, double x30, double y30,
		                                double x0, double y0, double a, double b, double nx, bool strict );
	static bool triangleWithinEllipse( double x10, double y10, double x20, double y20, double x30, double y30,
		                                double x0, double y0, double a, double b, double nx, bool strict );
 	static bool triangleWithinCircle( double x10, double y10, double x20, double y20, double x30, double y30,
									    double x0, double y0, double r, double nx, bool strict );
 	static bool triangleWithinPolygon( double x10, double y10, double x20, double y20, double x30, double y30,
									    const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict );
										
	// 2D ellipse
	static bool ellipseWithinTriangle( double px0, double py0, double a0, double b0, double nx0, 
										 double x1, double y1, double x2, double y2, double x3, double y3, bool strict );
	static bool ellipseWithinSquare( double px0, double py0, double a0, double b0, double nx0,
		                                double x0, double y0, double r, double nx, bool strict );
	static bool ellipseWithinRectangle( double px0, double py0, double a0, double b0, double nx0,
		                                double x0, double y0, double a, double b, double nx, bool strict );
	static bool ellipseWithinEllipse( double px0, double py0, double a0, double b0, double nx0,
		                                double x0, double y0, double a, double b, double nx, bool strict );
 	static bool ellipseWithinCircle( double px0, double py0, double a0, double b0, double nx0, 
									    double x0, double y0, double r, double nx, bool strict );
 	static bool ellipseWithinPolygon( double px0, double py0, double a0, double b0, double nx0, 
									    const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict );

	// rect 3D
	static bool rectangle3DWithinCube(  double px0, double py0, double pz0, double a0, double b0, double nx0, double ny0,
                                double x0, double y0, double z0, double r, double nx, double ny, bool strict );

	static bool rectangle3DWithinBox(  double px0, double py0, double pz0, double a0, double b0,
                                double nx0, double ny0,
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, bool strict );
	static bool rectangle3DWithinSphere(  double px0, double py0, double pz0, double a0, double b0,
                                       double nx0, double ny0,
                                       double x, double y, double z, double r, bool strict );
	static bool rectangle3DWithinEllipsoid(  double px0, double py0, double pz0, double a0, double b0,
                                    double nx0, double ny0,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, bool strict );
	static bool rectangle3DWithinCone(  double px0, double py0, double pz0, double a0, double b0,
                                    double nx0, double ny0,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, bool strict );


	// triangle 3D
	static bool triangle3DWithinCube(  double x10, double y10, double z10, double x20, double y20, double z20,
									double x30, double y30,  double z30,
									double x0, double y0, double z0, double r, double nx, double ny, bool strict );

	static bool triangle3DWithinBox(  double x10, double y10, double z10, double x20, double y20, double z20, 
									double x30, double y30, double z30,
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, bool strict );
	static bool triangle3DWithinSphere(  double x10, double y10, double z10, double x20, double y20, double z20,
									   double x30, double y30, double z30,
                                       double x, double y, double z, double r, bool strict );
	static bool triangle3DWithinEllipsoid(  double x10, double y10, double z10, double x20, double y20, double z20,
											double x30, double y30, double z30,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, bool strict );
	static bool triangle3DWithinCone(  double x10, double y10, double z10, double x20, double y20, double z20,
											double x30, double y30, double z30,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, bool strict );





	// 3D box
	static bool boxWithinCube(  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0,
	                            double x0, double y0, double z0, double r, double nx, double ny, bool strict );
	static bool boxWithinBox(  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
						       double x0, double y0, double z0, double w, double d, double h, double nx, double ny, bool strict );
	static bool boxWithinSphere( double px0, double py0, double pz0, double a0, double b0, double c0,
                                 double nx0, double ny0, double x, double y, double z, double r,
								 bool strict );
    static bool boxWithinEllipsoid(  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
                                    double x0, double y0, double z0, double w, double d, double h, 
									double nx, double ny, bool strict );
    static bool boxWithinCone(  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
                                    double x0, double y0, double z0, double r, double h, 
									double nx, double ny, bool strict );
	
	// ellipsoid
	static bool ellipsoidWithinCube(  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0,
	                            double x0, double y0, double z0, double r, double nx, double ny, bool strict );
	static bool ellipsoidWithinBox(  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
						       double x0, double y0, double z0, double w, double d, double h, double nx, double ny, bool strict );
	static bool ellipsoidWithinSphere( double px0, double py0, double pz0, double a0, double b0, double c0,
                                 double nx0, double ny0, double x, double y, double z, double r,
								 bool strict );
    static bool ellipsoidWithinEllipsoid(  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
                                    double x0, double y0, double z0, double w, double d, double h, 
									double nx, double ny, bool strict );
    static bool ellipsoidWithinCone(  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
                                    double x0, double y0, double z0, double r, double h, 
									double nx, double ny, bool strict );

	// 3D cyliner
	static bool cylinderWithinCube(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                               double x0, double y0, double z0, double r, double nx, double ny, bool strict );
	static bool cylinderWithinBox(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                                double x0, double y0, double z0, double w, double d, double h, 
										double nx, double ny, bool strict );
	static bool cylinderWithinSphere(  double px, double py, double pz, double pr0, double c0,  double nx0, double ny0,
										double x, double y, double z, double r, bool strict );
	static bool cylinderWithinEllipsoid(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                                    double x0, double y0, double z0, double w, double d, double h, 
											double nx, double ny, bool strict );
	static bool cylinderWithinCone(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                                    double x0, double y0, double z0, double r, double h, 
											double nx, double ny, bool strict );

	// 3D cone
	static bool coneWithinCube(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                               double x0, double y0, double z0, double r, double nx, double ny, bool strict );
	static bool coneWithinCube_test(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                               double x0, double y0, double z0, double r, double nx, double ny, bool strict );
	static bool coneWithinBox(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                                double x0, double y0, double z0, double w, double d, double h, 
										double nx, double ny, bool strict );
	static bool coneWithinSphere(  double px, double py, double pz, double pr0, double c0,  double nx0, double ny0,
										double x, double y, double z, double r, bool strict );
	static bool coneWithinEllipsoid(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                                    double x0, double y0, double z0, double w, double d, double h, 
											double nx, double ny, bool strict );
	static bool coneWithinCone(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                                    double x0, double y0, double z0, double r, double h, 
											double nx, double ny, bool strict );


	// 2D line
	static bool lineWithinTriangle( double x10, double y10, double x20, double y20, 
										 double x1, double y1, double x2, double y2, double x3, double y3,  bool strict );
	static bool lineWithinLineString( double x10, double y10, double x20, double y20, 
		                              const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict );
	static bool lineWithinSquare( double x10, double y10, double x20, double y20, 
		                                double x0, double y0, double r, double nx, bool strict );
	static bool lineWithinRectangle( double x10, double y10, double x20, double y20, 
		                                double x0, double y0, double a, double b, double nx, bool strict );
	static bool lineWithinEllipse( double x10, double y10, double x20, double y20, 
		                                double x0, double y0, double a, double b, double nx, bool strict );
 	static bool lineWithinCircle( double x10, double y10, double x20, double y20, 
									    double x0, double y0, double r, double nx, bool strict );
	static bool lineWithinPolygon( double x10, double y10, double x20, double y20, 
									const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict );


	// line 3D
	static bool line3DWithinLineString3D( double x10, double y10, double z10, double x20, double y20, double z20, 
		                              const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict );
	static bool line3DWithinCube(  double x10, double y10, double z10, double x20, double y20, double z20,
									double x0, double y0, double z0, double r, double nx, double ny, bool strict );

	static bool line3DWithinBox(  double x10, double y10, double z10, double x20, double y20, double z20, 
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, bool strict );
	static bool line3DWithinSphere(  double x10, double y10, double z10, double x20, double y20, double z20,
                                       double x, double y, double z, double r, bool strict );
	static bool line3DWithinEllipsoid(  double x10, double y10, double z10, double x20, double y20, double z20,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, bool strict );
	static bool line3DWithinCone(  double x10, double y10, double z10, double x20, double y20, double z20,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, bool strict );



	// linestring 2d
	static bool lineStringWithinLineString( const AbaxDataString &mk1, const JagStrSplit &sp1,
											const AbaxDataString &mk2, const JagStrSplit &sp2,  bool strict );
	static bool lineStringWithinTriangle( const AbaxDataString &mk1, const JagStrSplit &sp1,
										 double x1, double y1, double x2, double y2, double x3, double y3,  bool strict );
	static bool lineStringWithinSquare( const AbaxDataString &mk1, const JagStrSplit &sp1,
		                                double x0, double y0, double r, double nx, bool strict );
	static bool lineStringWithinRectangle( const AbaxDataString &mk1, const JagStrSplit &sp1,
		                                double x0, double y0, double a, double b, double nx, bool strict );
	static bool lineStringWithinEllipse( const AbaxDataString &mk1, const JagStrSplit &sp1,
		                                double x0, double y0, double a, double b, double nx, bool strict );
 	static bool lineStringWithinCircle( const AbaxDataString &mk1, const JagStrSplit &sp1,
									    double x0, double y0, double r, double nx, bool strict );
 	static bool lineStringWithinPolygon( const AbaxDataString &mk1, const JagStrSplit &sp1,
									     const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict );

	// linestring3d
	static bool lineString3DWithinLineString3D( const AbaxDataString &mk1, const JagStrSplit &sp1,
											    const AbaxDataString &mk2, const JagStrSplit &sp2,  bool strict );
	static bool lineString3DWithinCube( const AbaxDataString &mk1, const JagStrSplit &sp1,
									double x0, double y0, double z0, double r, double nx, double ny, bool strict );

	static bool lineString3DWithinBox(  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, bool strict );
	static bool lineString3DWithinSphere(  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                       double x, double y, double z, double r, bool strict );
	static bool lineString3DWithinEllipsoid(  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, bool strict );
	static bool lineString3DWithinCone(  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, bool strict );


	// polygon
	static bool polygonWithinTriangle( const AbaxDataString &mk1, const JagStrSplit &sp1,
										 double x1, double y1, double x2, double y2, double x3, double y3,  bool strict );
	static bool polygonWithinSquare( const AbaxDataString &mk1, const JagStrSplit &sp1,
		                                double x0, double y0, double r, double nx, bool strict );
	static bool polygonWithinRectangle( const AbaxDataString &mk1, const JagStrSplit &sp1,
		                                double x0, double y0, double a, double b, double nx, bool strict );
	static bool polygonWithinEllipse( const AbaxDataString &mk1, const JagStrSplit &sp1,
		                                double x0, double y0, double a, double b, double nx, bool strict );
 	static bool polygonWithinCircle( const AbaxDataString &mk1, const JagStrSplit &sp1,
									    double x0, double y0, double r, double nx, bool strict );
 	static bool polygonWithinPolygon( const AbaxDataString &mk1, const JagStrSplit &sp1,
										const AbaxDataString &mk2, const JagStrSplit &sp2 );

	// polygon3d within
	static bool polygon3DWithinCube( const AbaxDataString &mk1, const JagStrSplit &sp1,
									double x0, double y0, double z0, double r, double nx, double ny, bool strict );

	static bool polygon3DWithinBox(  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, bool strict );
	static bool polygon3DWithinSphere(  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                       double x, double y, double z, double r, bool strict );
	static bool polygon3DWithinEllipsoid(  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, bool strict );
	static bool polygon3DWithinCone(  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, bool strict );


	// multipolygon
	static bool multiPolygonWithinTriangle( const AbaxDataString &mk1, const JagStrSplit &sp1,
										 double x1, double y1, double x2, double y2, double x3, double y3,  bool strict );
	static bool multiPolygonWithinSquare( const AbaxDataString &mk1, const JagStrSplit &sp1,
		                                double x0, double y0, double r, double nx, bool strict );
	static bool multiPolygonWithinRectangle( const AbaxDataString &mk1, const JagStrSplit &sp1,
		                                double x0, double y0, double a, double b, double nx, bool strict );
	static bool multiPolygonWithinEllipse( const AbaxDataString &mk1, const JagStrSplit &sp1,
		                                double x0, double y0, double a, double b, double nx, bool strict );
 	static bool multiPolygonWithinCircle( const AbaxDataString &mk1, const JagStrSplit &sp1,
									    double x0, double y0, double r, double nx, bool strict );
 	static bool multiPolygonWithinPolygon( const AbaxDataString &mk1, const JagStrSplit &sp1,
										const AbaxDataString &mk2, const JagStrSplit &sp2 );

	// multipolygon3d within
	static bool multiPolygon3DWithinCube( const AbaxDataString &mk1, const JagStrSplit &sp1,
									double x0, double y0, double z0, double r, double nx, double ny, bool strict );

	static bool multiPolygon3DWithinBox(  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, bool strict );
	static bool multiPolygon3DWithinSphere(  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                       double x, double y, double z, double r, bool strict );
	static bool multiPolygon3DWithinEllipsoid(  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, bool strict );
	static bool multiPolygon3DWithinCone(  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, bool strict );



	static bool pointWithinNormalEllipse( double px, double py, double w, double h, bool strict );
	static bool point3DWithinNormalEllipsoid( double px, double py, double pz, 
											   double w, double d, double h, bool strict );
	static bool point3DWithinNormalCone( double px, double py, double pz, 
										 double r, double h, bool strict );

	/////////////////////////////////////// Intersect function ////////////////////////////////////////////////////
   	static bool doPointIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, int srid2, 
									const JagStrSplit &sp2, bool strict=true );
   	static bool doPoint3DIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, int srid2, 
								const JagStrSplit &sp2, bool strict=true );
   	static bool doTriangleIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, int srid2, 
									const JagStrSplit &sp2,  bool strict=true );
   	static bool doTriangle3DIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, int srid2, 
									const JagStrSplit &sp2,  bool strict=true );
   	static bool doCircleIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, int srid2, 
									const JagStrSplit &sp2,  bool strict=true );
   	static bool doCircle3DIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, int srid2, 
									const JagStrSplit &sp2,  bool strict=true );
   	static bool doSphereIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doBoxIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doRectangleIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doRectangle3DIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doSquareIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doSquare3DIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									  int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doCubeIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									 int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doCylinderIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									 int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doConeIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doEllipseIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doEllipsoidIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doLineIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doLine3DIntersect( int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, const AbaxDataString &colType2, 
									int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doLineStringIntersect( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, 
										const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doLineString3DIntersect( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, 
										const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doPolygonIntersect( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, 
										const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doPolygon3DIntersect( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, 
										const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doMultiPolygonIntersect( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, 
										const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, bool strict=true );
   	static bool doMultiPolygon3DIntersect( const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1, const AbaxDataString &mk2, 
										const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, bool strict=true );


	// 2D circle
	static bool circleIntersectCircle( double px, double py, double pr, double x, double y, double r, bool strict );
	static bool circleIntersectEllipse( double px, double py, double pr, 
									 	 double x, double y, double w, double h, double nx, 
									 	 bool strict=true, bool bound=true );
	static bool circleIntersectRectangle( double px0, double py0, double pr, double x0, double y0,
										  double w, double h, double nx,  bool strict );
	static bool circleIntersectTriangle( double px, double py, double pr, double x1, double y1, 
									  double x2, double y2, double x3, double y3, bool strict=true, bool bound = true );
	static bool circleIntersectPolygon( double px, double py, double pr, const AbaxDataString &mk2, const JagStrSplit &sp2,
									   bool strict=true );

	// 3D circle
	static bool circle3DIntersectBox( double px0, double py0, double pz0, double pr0,  double nx0, double ny0,
					                   double x0, double y0, double z0,  double a, double b, double c,
					                   double nx, double ny, bool strict );
   	static bool circle3DIntersectSphere( double px0, double py0, double pz0, double pr0,   double nx0, double ny0,
	   									 double x, double y, double z, double r, bool strict );
	static bool circle3DIntersectEllipsoid( double px0, double py0, double pz0, double pr0,  double nx0, double ny0,
										  double x0, double y0, double z0, 
									 	   double w, double d, double h, double nx, double ny, bool strict );
	static bool circle3DIntersectCone( double px0, double py0, double pz0, double pr0, double nx0, double ny0,
										  double x0, double y0, double z0, 
									 	   double r, double h, double nx, double ny, bool strict );

	// 2D square
	static bool squareIntersectTriangle( double px0, double py0, double pr0, double nx0, 
										 double x1, double y1, double x2, double y2, double x3, double y3, bool strict );
	static bool squareIntersectSquare( double px0, double py0, double pr0, double nx0,
		                                double x0, double y0, double r, double nx, bool strict );
	static bool squareIntersectRectangle( double px0, double py0, double pr0, double nx0,
		                                double x0, double y0, double a, double b, double nx, bool strict );
	static bool squareIntersectEllipse( double px0, double py0, double pr0, double nx0,
		                                double x0, double y0, double a, double b, double nx, bool strict );

 	static bool squareIntersectCircle( double px0, double py0, double pr0, double nx0, 
									    double x0, double y0, double r, double nx, bool strict );

	// 2D rectangle
	static bool rectangleIntersectTriangle( double px0, double py0, double a0, double b0, double nx0, 
										 double x1, double y1, double x2, double y2, double x3, double y3, bool strict );
	static bool rectangleIntersectRectangle( double px0, double py0, double a0, double b0, double nx0,
		                                double x0, double y0, double a, double b, double nx, bool strict );
	static bool rectangleIntersectEllipse( double px0, double py0, double a0, double b0, double nx0,
		                                double x0, double y0, double a, double b, double nx, bool strict );
	static bool rectangleIntersectPolygon( double px0, double py0, double a0, double b0, double nx0,
										   const AbaxDataString &mk2, const JagStrSplit &sp2 );

	// 2D triangle
	static bool triangleIntersectTriangle( double x10, double y10, double x20, double y20, double x30, double y30,
										 double x1, double y1, double x2, double y2, double x3, double y3, bool strict );
	static bool triangleIntersectRectangle( double x10, double y10, double x20, double y20, double x30, double y30,
		                                double x0, double y0, double a, double b, double nx, bool strict );
	static bool triangleIntersectEllipse( double x10, double y10, double x20, double y20, double x30, double y30,
		                                double x0, double y0, double a, double b, double nx, bool strict );
 	static bool triangleIntersectCircle( double x10, double y10, double x20, double y20, double x30, double y30,
									    double x0, double y0, double r, double nx, bool strict );
 	static bool triangleIntersectPolygon( double x10, double y10, double x20, double y20, double x30, double y30,
										  const AbaxDataString &mk2, const JagStrSplit &sp2 );
	static bool triangleIntersectLine( double x10, double y10, double x20, double y20, double x30, double y30,
										 double x1, double y1, double x2, double y2 );
 	static bool triangleIntersectLineString( double x10, double y10, double x20, double y20, double x30, double y30,
										  const AbaxDataString &mk2, const JagStrSplit &sp2 );
										
	// 2D ellipse
	static bool ellipseIntersectTriangle( double px0, double py0, double a0, double b0, double nx0, 
										 double x1, double y1, double x2, double y2, double x3, double y3, bool strict );
	static bool ellipseIntersectRectangle( double px0, double py0, double a0, double b0, double nx0,
		                                double x0, double y0, double a, double b, double nx, bool strict );
	static bool ellipseIntersectEllipse( double px0, double py0, double a0, double b0, double nx0,
		                                double x0, double y0, double a, double b, double nx, bool strict );
	static bool ellipseIntersectPolygon( double px0, double py0, double a0, double b0, double nx0,
										 const AbaxDataString &mk2, const JagStrSplit &sp2 );

	// rect 3D

	static bool rectangle3DIntersectBox(  double px0, double py0, double pz0, double a0, double b0,
                                double nx0, double ny0,
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, bool strict );
	static bool rectangle3DIntersectEllipsoid(  double px0, double py0, double pz0, double a0, double b0,
                                    double nx0, double ny0,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, bool strict );
	static bool rectangle3DIntersectCone(  double px0, double py0, double pz0, double a0, double b0,
                                    double nx0, double ny0,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, bool strict );


	// triangle 3D

	static bool triangle3DIntersectBox(  double x10, double y10, double z10, double x20, double y20, double z20, 
									double x30, double y30, double z30,
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, bool strict );
	static bool triangle3DIntersectEllipsoid(  double x10, double y10, double z10, double x20, double y20, double z20,
											double x30, double y30, double z30,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, bool strict );
	static bool triangle3DIntersectCone(  double x10, double y10, double z10, double x20, double y20, double z20,
											double x30, double y30, double z30,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, bool strict );





	// 3D box
	static bool boxIntersectBox(  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
						       double x0, double y0, double z0, double w, double d, double h, double nx, double ny, bool strict );
    static bool boxIntersectEllipsoid(  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
                                    double x0, double y0, double z0, double w, double d, double h, 
									double nx, double ny, bool strict );
    static bool boxIntersectCone(  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
                                    double x0, double y0, double z0, double r, double h, 
									double nx, double ny, bool strict );
	
	static bool ellipsoidIntersectBox(  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
						       double x0, double y0, double z0, double w, double d, double h, double nx, double ny, bool strict );
    static bool ellipsoidIntersectEllipsoid(  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
                                    double x0, double y0, double z0, double w, double d, double h, 
									double nx, double ny, bool strict );
    static bool ellipsoidIntersectCone(  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
                                    double x0, double y0, double z0, double r, double h, 
									double nx, double ny, bool strict );

	// 3D cyliner
	static bool cylinderIntersectBox(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                                double x0, double y0, double z0, double w, double d, double h, 
										double nx, double ny, bool strict );
	static bool cylinderIntersectEllipsoid(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                                    double x0, double y0, double z0, double w, double d, double h, 
											double nx, double ny, bool strict );
	static bool cylinderIntersectCone(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                                    double x0, double y0, double z0, double r, double h, 
											double nx, double ny, bool strict );

	// 3D cone
	static bool coneIntersectBox(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                                double x0, double y0, double z0, double w, double d, double h, 
										double nx, double ny, bool strict );
	static bool coneIntersectEllipsoid(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                                    double x0, double y0, double z0, double w, double d, double h, 
											double nx, double ny, bool strict );
	static bool coneIntersectCone(  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                                    double x0, double y0, double z0, double r, double h, 
											double nx, double ny, bool strict );


	// 2D line
	static bool lineIntersectTriangle( double x10, double y10, double x20, double y20, 
										 double x1, double y1, double x2, double y2, double x3, double y3,  bool strict );
	static bool lineIntersectLineString( double x10, double y10, double x20, double y20, 
		                              const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict );
	static bool lineIntersectRectangle( double x10, double y10, double x20, double y20, 
		                                double x0, double y0, double a, double b, double nx, bool strict );
	static bool lineIntersectEllipse( double x10, double y10, double x20, double y20, 
		                                double x0, double y0, double a, double b, double nx, bool strict );
	static bool lineIntersectLine(  double x1, double y1, double x2, double y2, double x3, double y3, double x4, double y4 );
	static bool lineIntersectLine(  const JagLine2D &line1, const JagLine2D &line2 );
	static bool lineIntersectPolygon( double x10, double y10, double x20, double y20, 
		                              const AbaxDataString &mk2, const JagStrSplit &sp2 );


	static bool line3DIntersectLineString3D( double x10, double y10, double z10, double x20, double y20, double z20,
		                              const AbaxDataString &mk2, const JagStrSplit &sp2, bool strict );
	static bool line3DIntersectBox( const JagLine3D &line, double x0, double y0, double z0,

									double w, double d, double h, double nx, double ny, bool strict );
	static bool line3DIntersectBox(  double x10, double y10, double z10, double x20, double y20, double z20, 
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, bool strict );
	static bool line3DIntersectSphere(  double x10, double y10, double z10, double x20, double y20, double z20,
                                       double x, double y, double z, double r, bool strict );
	static bool line3DIntersectEllipsoid(  double x10, double y10, double z10, double x20, double y20, double z20,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, bool strict );
	static bool line3DIntersectCone(  double x10, double y10, double z10, double x20, double y20, double z20,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, bool strict );
	static bool line3DIntersectCylinder(  double x10, double y10, double z10, double x20, double y20, double z20,
                                    double x0, double y0, double z0,
                                    double a, double b, double c, double nx, double ny, bool strict );

	// linestring intersect
	static bool lineStringIntersectLineString( const AbaxDataString &mk1, const JagStrSplit &sp1,
											const AbaxDataString &mk2, const JagStrSplit &sp2  );
	static bool lineStringIntersectTriangle( const AbaxDataString &m1, const JagStrSplit &sp1,
										 double x1, double y1, double x2, double y2, double x3, double y3,  bool strict );
	static bool lineStringIntersectRectangle( const AbaxDataString &m1, const JagStrSplit &sp1,
		                                double x0, double y0, double a, double b, double nx, bool strict );
	static bool lineStringIntersectEllipse( const AbaxDataString &m1, const JagStrSplit &sp1,
		                                double x0, double y0, double a, double b, double nx, bool strict );



	static bool lineString3DIntersectLineString3D( const AbaxDataString &mk1, const JagStrSplit &sp1,
											const AbaxDataString &mk2, const JagStrSplit &sp2  );

	static bool lineString3DIntersectBox(  const AbaxDataString &m1, const JagStrSplit &sp1,
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, bool strict );
	static bool lineString3DIntersectSphere(  const AbaxDataString &m1, const JagStrSplit &sp1,
                                       double x, double y, double z, double r, bool strict );
	static bool lineString3DIntersectEllipsoid(  const AbaxDataString &m1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, bool strict );
	static bool lineString3DIntersectCone(  const AbaxDataString &m1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, bool strict );
	static bool lineString3DIntersectCylinder( const AbaxDataString &m1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double a, double b, double c, double nx, double ny, bool strict );
	static bool lineString3DIntersectTriangle3D( const AbaxDataString &m1, const JagStrSplit &sp1,
												 const AbaxDataString &m2, const JagStrSplit &sp2 );
	static bool lineString3DIntersectSquare3D( const AbaxDataString &m1, const JagStrSplit &sp1,
												 const AbaxDataString &m2, const JagStrSplit &sp2 );
	static bool lineString3DIntersectRectangle3D( const AbaxDataString &m1, const JagStrSplit &sp1,
												 const AbaxDataString &m2, const JagStrSplit &sp2 );


	// polygon intersect
	static bool polygonIntersectLineString( const AbaxDataString &mk1, const JagStrSplit &sp1,
											const AbaxDataString &mk2, const JagStrSplit &sp2  );
	static bool polygonIntersectTriangle( const AbaxDataString &m1, const JagStrSplit &sp1,
										 double x1, double y1, double x2, double y2, double x3, double y3,  bool strict );
	static bool polygonIntersectRectangle( const AbaxDataString &m1, const JagStrSplit &sp1,
		                                double x0, double y0, double a, double b, double nx, bool strict );
	static bool polygonIntersectEllipse( const AbaxDataString &m1, const JagStrSplit &sp1,
		                                double x0, double y0, double a, double b, double nx, bool strict );
	static bool polygonIntersectLine( const AbaxDataString &m1, const JagStrSplit &sp1,
										 double x1, double y1, double x2, double y2 );

	// polygon3d intersect
	static bool polygon3DIntersectLineString3D( const AbaxDataString &mk1, const JagStrSplit &sp1,
											const AbaxDataString &mk2, const JagStrSplit &sp2  );

	static bool polygon3DIntersectBox(  const AbaxDataString &m1, const JagStrSplit &sp1,
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, bool strict );
	static bool polygon3DIntersectSphere(  const AbaxDataString &m1, const JagStrSplit &sp1,
                                       double x, double y, double z, double r, bool strict );
	static bool polygon3DIntersectEllipsoid(  const AbaxDataString &m1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, bool strict );
	static bool polygon3DIntersectCone(  const AbaxDataString &m1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, bool strict );
	static bool polygon3DIntersectCylinder( const AbaxDataString &m1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double a, double b, double c, double nx, double ny, bool strict );

	// 2D area or 3D surface area
   	static double doCircleArea(  int srid1, const JagStrSplit &sp1 );
   	static double doCircle3DArea(  int srid1, const JagStrSplit &sp1 );
   	static double doSphereArea(  int srid1, const JagStrSplit &sp1 );
   	static double doSquareArea(  int srid1, const JagStrSplit &sp1 );
   	static double doSquare3DArea(  int srid1, const JagStrSplit &sp1 );
   	static double doCubeArea(  int srid1, const JagStrSplit &sp1 );
   	static double doRectangleArea(  int srid1, const JagStrSplit &sp1 );
   	static double doRectangle3DArea(  int srid1, const JagStrSplit &sp1 );
   	static double doBoxArea(  int srid1, const JagStrSplit &sp1 );
   	static double doTriangleArea(  int srid1, const JagStrSplit &sp1 );
   	static double doTriangle3DArea(  int srid1, const JagStrSplit &sp1 );
   	static double doCylinderArea(  int srid1, const JagStrSplit &sp1 );
   	static double doConeArea(  int srid1, const JagStrSplit &sp1 );
   	static double doEllipseArea(  int srid1, const JagStrSplit &sp1 );
   	static double doEllipse3DArea(  int srid1, const JagStrSplit &sp1 );
   	static double doEllipsoidArea(  int srid1, const JagStrSplit &sp1 );
   	static double doPolygonArea(  const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1 );
   	static double doMultiPolygonArea(  const AbaxDataString &mk1, int srid1, const JagStrSplit &sp1 );


	/*****
	// polygon intersect
	static bool multiPolygonIntersectLineString( const AbaxDataString &mk1, const JagStrSplit &sp1,
											const AbaxDataString &mk2, const JagStrSplit &sp2  );
	static bool multiPolygonIntersectTriangle( const AbaxDataString &m1, const JagStrSplit &sp1,
										 double x1, double y1, double x2, double y2, double x3, double y3,  bool strict );
	static bool multiPolygonIntersectRectangle( const AbaxDataString &m1, const JagStrSplit &sp1,
		                                double x0, double y0, double a, double b, double nx, bool strict );
	static bool multiPolygonIntersectEllipse( const AbaxDataString &m1, const JagStrSplit &sp1,
		                                double x0, double y0, double a, double b, double nx, bool strict );

	// multiPolygon3d intersect
	static bool multiPolygon3DIntersectLineString3D( const AbaxDataString &mk1, const JagStrSplit &sp1,
											const AbaxDataString &mk2, const JagStrSplit &sp2  );

	static bool multiPolygon3DIntersectBox(  const AbaxDataString &m1, const JagStrSplit &sp1,
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, bool strict );
	static bool multiPolygon3DIntersectSphere(  const AbaxDataString &m1, const JagStrSplit &sp1,
                                       double x, double y, double z, double r, bool strict );
	static bool multiPolygon3DIntersectEllipsoid(  const AbaxDataString &m1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, bool strict );
	static bool multiPolygon3DIntersectCone(  const AbaxDataString &m1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, bool strict );
	static bool multiPolygon3DIntersectCylinder( const AbaxDataString &m1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double a, double b, double c, double nx, double ny, bool strict );
	*********/

	////////////////////////////////////////////////////////////////////////////////////////////////////////
	// misc
	static double distance( double fx, double fy, double gx, double gy, int srid );
	static double distance( double fx, double fy, double fz, double gx, double gy, double gz, int srid );
	static double squaredDistance( double fx, double fy, double gx, double gy, int srid );
	static double squaredDistance( double fx, double fy, double fz, double gx, double gy, double gz, int srid );

    static double DistanceOfPointToLine(double x0 ,double y0 ,double z0 ,double x1 ,double y1 ,double z1 ,double x2 ,double y2 ,double z2);


    static bool twoLinesIntersection( double slope1, double c1, double slope2, double c2,
									  double &outx, double &outy );
	static void orginBoundingBoxOfRotatedEllipse( double a, double b, double nx, 
										double &x1, double &y1,
										double &x2, double &y2,
										double &x3, double &y3,
										double &x4, double &y4 );
	static void boundingBoxOfRotatedEllipse( double x0, double y0, double a, double b, double nx, 
										double &x1, double &y1,
										double &x2, double &y2,
										double &x3, double &y3,
										double &x4, double &y4 );
		
	static void project3DCircleToXYPlane( double x0, double y0, double z0, double r0, double nx0, double ny0,
									 double x, double y, double a, double b, double nx );
	static void project3DCircleToYZPlane( double x0, double y0, double z0, double r0, double nx0, double ny0,
									 double y, double z, double a, double b, double ny );
	static void project3DCircleToZXPlane( double x0, double y0, double z0, double r0, double nx0, double ny0,
									 double z, double x, double a, double b, double nz );

	static void transform3DDirection( double nx1, double ny1, double nx2, double ny2, double &nx, double &ny );
	static void transform2DDirection( double nx1, double nx2, double &nx );
	static void samplesOn2DCircle( double x0, double y0, double r, int num, JagVector<JagPoint2D> &samples );
	static void samplesOn3DCircle( double x0, double y0, double z0, double r, double nx, double ny, 
								   int num, JagVector<JagPoint3D> &samples );

	static void samplesOn2DEllipse( double x0, double y0, double a, double b, double nx, int num, JagVector<JagPoint2D> &samples );
	static void samplesOnEllipsoid( double x0, double y0, double z0, double a, double b, double c,
									   double nx, double ny, int num2, JagVector<JagPoint3D> &samples );
	static void sampleLinesOnCone( double x0, double y0, double z0, double r, double h,
									   double nx, double ny, int num2, JagVector<JagLine3D> &samples );
	static void sampleLinesOnCylinder( double x0, double y0, double z0, double r, double h,
									   double nx, double ny, int num2, JagVector<JagLine3D> &samples );

	static AbaxDataString convertType2Short( const AbaxDataString &geotypeLong );
	static bool jagLE (double f1, double f2 );
	static bool jagGE (double f1, double f2 );
	static bool jagEQ (double f1, double f2 );
	static bool jagSquare(double f );

	static void rotate2DCoordGlobal2Local( double inx, double iny, double nx, double &outx, double &outy );
	static void transform2DCoordGlobal2Local( double outx0, double outy0, double inx, double iny, double nx,
                               	      double &outx, double &outy );
	static void rotate2DCoordLocal2Global( double inx, double iny, double nx, double &outx, double &outy );
	static void transform2DCoordLocal2Global( double inx0, double iny0, double inx, double iny, double nx,
                               	      double &outx, double &outy );


	static void rotate3DCoordGlobal2Local( double inx, double iny, double inz, double nx, double ny, 
								    double &outx, double &outy, double &outz );
    static void transform3DCoordGlobal2Local( double outx0, double outy0, double outz0, double inx, double iny, double inz,
									  double nx,  double ny, double &outx, double &outy, double &outz );
	static void rotate3DCoordLocal2Global( double inx, double iny, double inz, double nx, double ny, 
								    double &outx, double &outy, double &outz );
    static void transform3DCoordLocal2Global( double inx0, double iny0, double inz0, double inx, double iny, double inz,
									  double nx,  double ny, double &outx, double &outy, double &outz );
   static bool doAllNearby( const AbaxDataString& mark1, const AbaxDataString &colType1, int srid1, const JagStrSplit &sp1,
                             const AbaxDataString& mark2, const AbaxDataString &colType2, int srid2, const JagStrSplit &sp2, 
							 const AbaxDataString &carg );

    static int getDimension( const AbaxDataString& colType );
    static AbaxDataString getTypeStr( const AbaxDataString& colType );
    static int getPolyDimension( const AbaxDataString& colType );
	template <class POINT> static int sortLinePoints( POINT arr[], int elements );
	static bool above(double x, double y, double x2, double y2, double x3, double y3 );
	static bool above(double x, double y, double z, double x2, double y2, double z2, double x3, double y3, double z3 );
	static bool aboveOrSame(double x, double y, double x2, double y2, double x3, double y3 );
	static bool aboveOrSame(double x, double y, double z, double x2, double y2, double z2, double x3, double y3, double z3 );
	static bool below(double x, double y, double x2, double y2, double x3, double y3 );
	static bool below(double x, double y, double z, double x2, double y2, double z2, double x3, double y3, double z3 );
	static bool belowOrSame(double x, double y, double x2, double y2, double x3, double y3 );
	static bool belowOrSame(double x, double y, double z, double x2, double y2, double z2, double x3, double y3, double z3 );
	static bool same(double x, double y, double x2, double y2, double x3, double y3 );
	static bool same(double x, double y, double z, double x2, double y2, double z2, double x3, double y3, double z3 );
	static bool isNull( double x, double y );
	static bool isNull( double x, double y, double z );
	static bool isNull(double x1, double y1, double x2, double y2 );
	static bool isNull(double x1, double y1, double z1, double x2, double y2, double z2 );
	static AbaxDataString makeGeoJson( const JagStrSplit &sp, const char *str );
	static AbaxDataString makeJsonLineString( const AbaxDataString &title, const JagStrSplit &sp, const char *str );
	static AbaxDataString makeJsonLineString3D( const AbaxDataString &title, const JagStrSplit &sp, const char *str );
	static AbaxDataString makeJsonPolygon( const AbaxDataString &title, const JagStrSplit &sp, const char *str, bool is3D );
	static AbaxDataString makeJsonMultiPolygon( const AbaxDataString &title, const JagStrSplit &sp, const char *str, bool is3D );
	static AbaxDataString makeJsonDefault( const JagStrSplit &sp, const char *str );
	static bool distance( const AbaxFixString &lstr, const AbaxFixString &rstr, const AbaxDataString &arg, double &dist );


	////////////// distance //////////////////

	static bool doPointDistance( const AbaxDataString& mark1, const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist );
	static bool doPoint3DDistance( const AbaxDataString& mark1, const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doCircleDistance( const AbaxDataString& mark1,  const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doCircle3DDistance( const AbaxDataString& mark1,  const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doSphereDistance( const AbaxDataString& mark1,  const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doSquareDistance( const AbaxDataString& mark1,  const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doSquare3DDistance( const AbaxDataString& mark1,  const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doCubeDistance( const AbaxDataString& mark1,  const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doRectangleDistance( const AbaxDataString& mark1,  const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doRectangle3DDistance( const AbaxDataString& mark1,  const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doBoxDistance( const AbaxDataString& mark1,  const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doTriangleDistance( const AbaxDataString& mark1,  const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doTriangle3DDistance( const AbaxDataString& mark1,  const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doCylinderDistance( const AbaxDataString& mark1,  const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doConeDistance( const AbaxDataString& mark1,  const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doEllipseDistance( const AbaxDataString& mark1,  const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doEllipsoidDistance( const AbaxDataString& mark1,  const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doLineDistance( const AbaxDataString& mark1,  const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doLine3DDistance( const AbaxDataString& mark1,  const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doLineStringDistance( const AbaxDataString& mark1, const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doLineString3DDistance( const AbaxDataString& mark1, const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doPolygonDistance( const AbaxDataString& mark1, const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doPolygon3DDistance( const AbaxDataString& mark1, const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doMultiPolygonDistance( const AbaxDataString& mark1, const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	static bool doMultiPolygon3DDistance( const AbaxDataString& mark1, const JagStrSplit& sp1, const AbaxDataString& mark2,
										 const AbaxDataString& colType2,
								 const JagStrSplit& sp2, int srid, const AbaxDataString& arg, double &dist);
	
	// 2D point
	static bool pointDistancePoint( int srid, double px, double py, double x1, double y1, const AbaxDataString& arg, double &dist );
	static bool pointDistanceLine( int srid, double px, double py, double x1, double y1, double x2, double y2, const AbaxDataString& arg, double &dist );
	static bool pointDistanceLineString( int srid,  double x, double y, const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist );
	static bool pointDistanceSquare( int srid, double px, double py, double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist );
	static bool pointDistanceCircle( int srid, double px, double py, double x0, double y0, double r, const AbaxDataString& arg, double &dist );
	static bool pointDistanceRectangle( int srid, double px, double py, double x, double y, double a, double b, double nx, const AbaxDataString& arg, double &dist );
	static bool pointDistanceEllipse( int srid, double px, double py, double x, double y, double a, double b, double nx, const AbaxDataString& arg, double &dist );
	static bool pointDistanceTriangle( int srid, double px, double py, double x1, double y1,
									  double x2, double y2, double x3, double y3,
					 				  const AbaxDataString& arg, double &dist ); 
	static bool pointDistancePolygon( int srid, double x, double y, const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist );
	static bool pointDistancePolygon( int srid, double x, double y, const JagLineString3D &linestr );
	static bool pointDistancePolygon( int srid, double x, double y, const JagPolygon &pgon );

	// 3D point
	static bool point3DDistancePoint3D( int srid, double px, double py, double pz, double x1, double y1, double z1, const AbaxDataString& arg, double &dist );
	static bool point3DDistanceLine3D( int srid, double px, double py, double pz, double x1, double y1, double z1, 
									double x2, double y2, double z2, const AbaxDataString& arg, double &dist );
	static bool point3DDistanceLineString3D( int srid,  double x, double y, double z, const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist );
	static bool point3DDistanceBox( int srid, double px, double py, double pz,  
									  double x, double y, double z, double a, double b, double c, double nx, double ny, 
									  const AbaxDataString& arg, double &dist );
    static bool point3DDistanceSphere( int srid, double px, double py, double pz, double x, double y, double z, double r, const AbaxDataString& arg, double &dist );
	static bool point3DDistanceEllipsoid( int srid, double px, double py, double pz,  
									  double x, double y, double z, double a, double b, double c, double nx, double ny, 
									  const AbaxDataString& arg, double &dist );

	static bool point3DDistanceCone( int srid, double px, double py, double pz, 
									double x0, double y0, double z0,
									 double r, double h,  double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool point3DDistanceSquare3D( int srid, double px, double py, double pz, 
									double x0, double y0, double z0,
									 double a, double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool point3DDistanceCylinder( int srid,  double x, double y, double z,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool point3DDistanceNormalCylinder( int srid,  double x, double y, double z,
                                    double r, double h, const AbaxDataString& arg, double &dist );

	// 2D circle
	static bool circleDistanceCircle( int srid, double px, double py, double pr, double x, double y, double r, const AbaxDataString& arg, double &dist );
	static bool circleDistanceSquare( int srid, double px0, double py0, double pr, double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist );
	static bool circleDistanceEllipse( int srid, double px, double py, double pr, 
									 	 double x, double y, double w, double h, double nx, 
									 	 const AbaxDataString& arg, double &dist );
	static bool circleDistanceRectangle( int srid, double px0, double py0, double pr, double x0, double y0,
										  double w, double h, double nx,  const AbaxDataString& arg, double &dist );
	static bool circleDistanceTriangle( int srid, double px, double py, double pr, double x1, double y1, 
									  double x2, double y2, double x3, double y3, const AbaxDataString& arg, double &dist );
	static bool circleDistancePolygon( int srid, double px0, double py0, double pr, 
									 const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist );

	// 3D circle
	static bool circle3DDistanceCube( int srid, double px0, double py0, double pz0, double pr0,  double nx0, double ny0,
										double x0, double y0, double z0,  double r, double nx, double ny, const AbaxDataString& arg, 
										double &dist );

	static bool circle3DDistanceBox( int srid, double px0, double py0, double pz0, double pr0,  double nx0, double ny0,
					                   double x0, double y0, double z0,  double a, double b, double c,
					                   double nx, double ny, const AbaxDataString& arg, double &dist );
   	static bool circle3DDistanceSphere( int srid, double px0, double py0, double pz0, double pr0,   double nx0, double ny0,
	   									 double x, double y, double z, double r, const AbaxDataString& arg, double &dist );
	static bool circle3DDistanceEllipsoid( int srid, double px0, double py0, double pz0, double pr0,  double nx0, double ny0,
										  double x0, double y0, double z0, 
									 	   double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool circle3DDistanceCone( int srid, double px0, double py0, double pz0, double pr0, double nx0, double ny0,
										  double x0, double y0, double z0, 
									 	   double r, double h, double nx, double ny, const AbaxDataString& arg, double &dist );

	// 3D sphere
	static bool sphereDistanceCube( int srid,  double px0, double py0, double pz0, double pr0,
		                               double x0, double y0, double z0, double r, double nx, double ny, 
										const AbaxDataString& arg, double &dist );
	static bool sphereDistanceBox( int srid,  double px0, double py0, double pz0, double r,
		                                double x0, double y0, double z0, double w, double d, double h, 
										double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool sphereDistanceSphere( int srid,  double px, double py, double pz, double pr, 
										double x, double y, double z, double r, const AbaxDataString& arg, double &dist );
	static bool sphereDistanceEllipsoid( int srid,  double px0, double py0, double pz0, double pr,
		                                    double x0, double y0, double z0, double w, double d, double h, 
											double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool sphereDistanceCone( int srid,  double px0, double py0, double pz0, double pr,
		                                    double x0, double y0, double z0, double r, double h, 
											double nx, double ny, const AbaxDataString& arg, double &dist );

	// 2D rectangle
	static bool rectangleDistanceTriangle( int srid, double px0, double py0, double a0, double b0, double nx0, 
										 double x1, double y1, double x2, double y2, double x3, double y3, const AbaxDataString& arg, double &dist );
	static bool rectangleDistanceSquare( int srid, double px0, double py0, double a0, double b0, double nx0,
		                                double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist );
	static bool rectangleDistanceRectangle( int srid, double px0, double py0, double a0, double b0, double nx0,
		                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist );
	static bool rectangleDistanceEllipse( int srid, double px0, double py0, double a0, double b0, double nx0,
		                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist );
 	static bool rectangleDistanceCircle( int srid, double px0, double py0, double a0, double b0, double nx0, 
									    double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist );
 	static bool rectangleDistancePolygon( int srid, double px0, double py0, double a0, double b0, double nx0, 
										const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist );

	// 2D triangle
	static bool triangleDistanceTriangle( int srid, double x10, double y10, double x20, double y20, double x30, double y30,
										 double x1, double y1, double x2, double y2, double x3, double y3, const AbaxDataString& arg, double &dist );
	static bool triangleDistanceSquare( int srid, double x10, double y10, double x20, double y20, double x30, double y30,
		                                double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist );
	static bool triangleDistanceRectangle( int srid, double x10, double y10, double x20, double y20, double x30, double y30,
		                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist );
	static bool triangleDistanceEllipse( int srid, double x10, double y10, double x20, double y20, double x30, double y30,
		                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist );
 	static bool triangleDistanceCircle( int srid, double x10, double y10, double x20, double y20, double x30, double y30,
									    double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist );
 	static bool triangleDistancePolygon( int srid, double x10, double y10, double x20, double y20, double x30, double y30,
									    const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist );
										
	// 2D ellipse
	static bool ellipseDistanceTriangle( int srid, double px0, double py0, double a0, double b0, double nx0, 
										 double x1, double y1, double x2, double y2, double x3, double y3, const AbaxDataString& arg, double &dist );
	static bool ellipseDistanceSquare( int srid, double px0, double py0, double a0, double b0, double nx0,
		                                double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist );
	static bool ellipseDistanceRectangle( int srid, double px0, double py0, double a0, double b0, double nx0,
		                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist );
	static bool ellipseDistanceEllipse( int srid, double px0, double py0, double a0, double b0, double nx0,
		                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist );
 	static bool ellipseDistanceCircle( int srid, double px0, double py0, double a0, double b0, double nx0, 
									    double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist );
 	static bool ellipseDistancePolygon( int srid, double px0, double py0, double a0, double b0, double nx0, 
									    const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist );

	// rect 3D
	static bool rectangle3DDistanceCube( int srid,  double px0, double py0, double pz0, double a0, double b0, double nx0, double ny0,
                                double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist );

	static bool rectangle3DDistanceBox( int srid,  double px0, double py0, double pz0, double a0, double b0,
                                double nx0, double ny0,
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool rectangle3DDistanceSphere( int srid,  double px0, double py0, double pz0, double a0, double b0,
                                       double nx0, double ny0,
                                       double x, double y, double z, double r, const AbaxDataString& arg, double &dist );
	static bool rectangle3DDistanceEllipsoid( int srid,  double px0, double py0, double pz0, double a0, double b0,
                                    double nx0, double ny0,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool rectangle3DDistanceCone( int srid,  double px0, double py0, double pz0, double a0, double b0,
                                    double nx0, double ny0,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, const AbaxDataString& arg, double &dist );


	// triangle 3D
	static bool triangle3DDistanceCube( int srid,  double x10, double y10, double z10, double x20, double y20, double z20,
									double x30, double y30,  double z30,
									double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist );

	static bool triangle3DDistanceBox( int srid,  double x10, double y10, double z10, double x20, double y20, double z20, 
									double x30, double y30, double z30,
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool triangle3DDistanceSphere( int srid,  double x10, double y10, double z10, double x20, double y20, double z20,
									   double x30, double y30, double z30,
                                       double x, double y, double z, double r, const AbaxDataString& arg, double &dist );
	static bool triangle3DDistanceEllipsoid( int srid,  double x10, double y10, double z10, double x20, double y20, double z20,
											double x30, double y30, double z30,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool triangle3DDistanceCone( int srid,  double x10, double y10, double z10, double x20, double y20, double z20,
											double x30, double y30, double z30,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, const AbaxDataString& arg, double &dist );





	// 3D box
	static bool boxDistanceCube( int srid,  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0,
	                            double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool boxDistanceBox( int srid,  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
						       double x0, double y0, double z0, double w, double d, double h, double nx, double ny, 
								const AbaxDataString& arg, double &dist );
	static bool boxDistanceSphere( int srid, double px0, double py0, double pz0, double a0, double b0, double c0,
                                 double nx0, double ny0, double x, double y, double z, double r,
								 const AbaxDataString& arg, double &dist );
    static bool boxDistanceEllipsoid( int srid,  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
                                    double x0, double y0, double z0, double w, double d, double h, 
									double nx, double ny, const AbaxDataString& arg, double &dist );
    static bool boxDistanceCone( int srid,  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
                                    double x0, double y0, double z0, double r, double h, 
									double nx, double ny, const AbaxDataString& arg, double &dist );
	
	// ellipsoid
	static bool ellipsoidDistanceCube( int srid,  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0,
	                            double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool ellipsoidDistanceBox( int srid,  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
						       double x0, double y0, double z0, double w, double d, double h, double nx, double ny, 
							   const AbaxDataString& arg, double &dist );
	static bool ellipsoidDistanceSphere( int srid, double px0, double py0, double pz0, double a0, double b0, double c0,
                                 double nx0, double ny0, double x, double y, double z, double r,
								 const AbaxDataString& arg, double &dist );
    static bool ellipsoidDistanceEllipsoid( int srid,  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
                                    double x0, double y0, double z0, double w, double d, double h, 
									double nx, double ny, const AbaxDataString& arg, double &dist );
    static bool ellipsoidDistanceCone( int srid,  double px0, double py0, double pz0, double a0, double b0, double c0, double nx0, double ny0, 
                                    double x0, double y0, double z0, double r, double h, 
									double nx, double ny, const AbaxDataString& arg, double &dist );

	// 3D cyliner
	static bool cylinderDistanceCube( int srid,  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                               double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool cylinderDistanceBox( int srid,  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                                double x0, double y0, double z0, double w, double d, double h, 
										double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool cylinderDistanceSphere( int srid,  double px, double py, double pz, double pr0, double c0,  double nx0, double ny0,
										double x, double y, double z, double r, const AbaxDataString& arg, double &dist );
	static bool cylinderDistanceEllipsoid( int srid,  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                                    double x0, double y0, double z0, double w, double d, double h, 
											double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool cylinderDistanceCone( int srid,  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                                    double x0, double y0, double z0, double r, double h, 
											double nx, double ny, const AbaxDataString& arg, double &dist );

	// 3D cone
	static bool coneDistanceCube( int srid,  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                               double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool coneDistanceCube_test( int srid,  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                               double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool coneDistanceBox( int srid,  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                                double x0, double y0, double z0, double w, double d, double h, 
										double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool coneDistanceSphere( int srid,  double px, double py, double pz, double pr0, double c0,  double nx0, double ny0,
										double x, double y, double z, double r, const AbaxDataString& arg, double &dist );
	static bool coneDistanceEllipsoid( int srid,  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                                    double x0, double y0, double z0, double w, double d, double h, 
											double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool coneDistanceCone( int srid,  double px0, double py0, double pz0, double pr0, double c0, double nx0, double ny0,
		                                    double x0, double y0, double z0, double r, double h, 
											double nx, double ny, const AbaxDataString& arg, double &dist );


	// 2D line
	static bool lineDistanceTriangle( int srid, double x10, double y10, double x20, double y20, 
										 double x1, double y1, double x2, double y2, double x3, double y3,  const AbaxDataString& arg, double &dist );
	static bool lineDistanceLineString( int srid, double x10, double y10, double x20, double y20, 
		                              const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist );
	static bool lineDistanceSquare( int srid, double x10, double y10, double x20, double y20, 
		                                double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist );
	static bool lineDistanceRectangle( int srid, double x10, double y10, double x20, double y20, 
		                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist );
	static bool lineDistanceEllipse( int srid, double x10, double y10, double x20, double y20, 
		                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist );
 	static bool lineDistanceCircle( int srid, double x10, double y10, double x20, double y20, 
									    double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist );
	static bool lineDistancePolygon( int srid, double x10, double y10, double x20, double y20, 
									const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist );


	// line 3D
	static bool line3DDistanceLineString3D( int srid, double x10, double y10, double z10, double x20, double y20, double z20, 
		                              const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist );
	static bool line3DDistanceCube( int srid,  double x10, double y10, double z10, double x20, double y20, double z20,
									double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist );

	static bool line3DDistanceBox( int srid,  double x10, double y10, double z10, double x20, double y20, double z20, 
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool line3DDistanceSphere( int srid,  double x10, double y10, double z10, double x20, double y20, double z20,
                                       double x, double y, double z, double r, const AbaxDataString& arg, double &dist );
	static bool line3DDistanceEllipsoid( int srid,  double x10, double y10, double z10, double x20, double y20, double z20,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool line3DDistanceCone( int srid,  double x10, double y10, double z10, double x20, double y20, double z20,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, const AbaxDataString& arg, double &dist );



	// linestring 2d
	static bool lineStringDistanceLineString( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
											const AbaxDataString &mk2, const JagStrSplit &sp2,  const AbaxDataString& arg, double &dist );
	static bool lineStringDistanceTriangle( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
										 double x1, double y1, double x2, double y2, double x3, double y3,  const AbaxDataString& arg, double &dist );
	static bool lineStringDistanceSquare( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
		                                double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist );
	static bool lineStringDistanceRectangle( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
		                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist );
	static bool lineStringDistanceEllipse( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
		                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist );
 	static bool lineStringDistanceCircle( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
									    double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist );
 	static bool lineStringDistancePolygon( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
									     const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist );

	// linestring3d
	static bool lineString3DDistanceLineString3D( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
											    const AbaxDataString &mk2, const JagStrSplit &sp2,  const AbaxDataString& arg, double &dist );
	static bool lineString3DDistanceCube( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
									double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist );

	static bool lineString3DDistanceBox( int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool lineString3DDistanceSphere( int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                       double x, double y, double z, double r, const AbaxDataString& arg, double &dist );
	static bool lineString3DDistanceEllipsoid( int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool lineString3DDistanceCone( int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, const AbaxDataString& arg, double &dist );


	// polygon
	static bool polygonDistanceTriangle( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
										 double x1, double y1, double x2, double y2, double x3, double y3,  const AbaxDataString& arg, double &dist );
	static bool polygonDistanceSquare( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
		                                double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist );
	static bool polygonDistanceRectangle( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
		                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist );
	static bool polygonDistanceEllipse( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
		                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist );
 	static bool polygonDistanceCircle( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
									    double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist );
 	static bool polygonDistancePolygon( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
										const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist );

	// polygon3d Distance
	static bool polygon3DDistanceCube( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
									double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist );

	static bool polygon3DDistanceBox( int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool polygon3DDistanceSphere( int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                       double x, double y, double z, double r, const AbaxDataString& arg, double &dist );
	static bool polygon3DDistanceEllipsoid( int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool polygon3DDistanceCone( int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, const AbaxDataString& arg, double &dist );


	// multipolygon
	static bool multiPolygonDistanceTriangle( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
										 double x1, double y1, double x2, double y2, double x3, double y3,  const AbaxDataString& arg, double &dist );
	static bool multiPolygonDistanceSquare( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
		                                double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist );
	static bool multiPolygonDistanceRectangle( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
		                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist );
	static bool multiPolygonDistanceEllipse( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
		                                double x0, double y0, double a, double b, double nx, const AbaxDataString& arg, double &dist );
 	static bool multiPolygonDistanceCircle( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
									    double x0, double y0, double r, double nx, const AbaxDataString& arg, double &dist );
 	static bool multiPolygonDistancePolygon( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
										const AbaxDataString &mk2, const JagStrSplit &sp2, const AbaxDataString& arg, double &dist );

	// multipolygon3d Distance
	static bool multiPolygon3DDistanceCube( int srid, const AbaxDataString &mk1, const JagStrSplit &sp1,
									double x0, double y0, double z0, double r, double nx, double ny, const AbaxDataString& arg, double &dist );

	static bool multiPolygon3DDistanceBox( int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                double x0, double y0, double z0,
                                double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool multiPolygon3DDistanceSphere( int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                       double x, double y, double z, double r, const AbaxDataString& arg, double &dist );
	static bool multiPolygon3DDistanceEllipsoid( int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double w, double d, double h, double nx, double ny, const AbaxDataString& arg, double &dist );
	static bool multiPolygon3DDistanceCone( int srid,  const AbaxDataString &mk1, const JagStrSplit &sp1,
                                    double x0, double y0, double z0,
                                    double r, double h, double nx, double ny, const AbaxDataString& arg, double &dist );



	static bool pointDistanceNormalEllipse( int srid, double px, double py, double w, double h, const AbaxDataString& arg, double &dist );
	static bool point3DDistanceNormalEllipsoid( int srid, double px, double py, double pz, 
											   double w, double d, double h, const AbaxDataString& arg, double &dist );
	static bool point3DDistanceNormalCone( int srid, double px, double py, double pz, 
										 double r, double h, const AbaxDataString& arg, double &dist );

	static double pointToLineDistance( double lata1, double lona1, double lata2, double lona2, double latb1, double lonb1 );
	static double safeget( const JagStrSplit &sp, int arg );
	static AbaxDataString safeGetStr( const JagStrSplit &sp, int arg );
	static void center2DMultiPolygon( const JagVector<JagPolygon> &pgvec, double &cx, double &cy );
	static void center3DMultiPolygon( const JagVector<JagPolygon> &pgvec, double &cx, double &cy, double &cz );



  protected:
	static const double ZERO;
	static const double NUM_SAMPLE;
	static double doSign( const JagPoint2D &p1, const JagPoint2D &p2, const JagPoint2D &p3 );
	static double distSquarePointToSeg( const JagPoint2D &p,
										const JagPoint2D &p1, const JagPoint2D &p2 );
	static bool locIn3DCenterBox( double x, double y, double z, double a, double b, double c, bool strict ); 
	static bool locIn2DCenterBox( double x, double y, double a, double b, bool strict ); 
	static bool validDirection( double nx );
	static bool validDirection( double nx, double ny );

	static bool pointIntersectNormalEllipse( double px, double py, double w, double h, bool strict );
	static bool point3DIntersectNormalEllipsoid( double px, double py, double pz, 
											   double w, double d, double h, bool strict );
	static bool point3DIntersectNormalCone( double px, double py, double pz, 
										 double r, double h, bool strict );

	static double jagMin( double x1, double x2, double x3 );
	static double jagMax( double x1, double x2, double x3 );
	static bool bound2DDisjoint( double px1, double py1, double w1, double h1, double px2, double py2, double w2, double h2 );
	static bool bound3DDisjoint( 
				double px1, double py1, double pz1, double w1, double d1, double h1,
				double px2, double py2, double pz2, double w2, double d2, double h2 );

	static bool bound2DLineBoxDisjoint( double x10, double y10, double x20, double y20, 
				 						double x0, double y0, double w, double h );

	static bool bound3DLineBoxDisjoint( double x10, double y10, double z10,
				 						double x20, double y20, double z20, 
				 						double x0, double y0, double z0, double w, double d, double h );

	static bool bound2DTriangleDisjoint( double px1, double py1, double px2, double py2, double px3, double py3,
									     double px0, double py0, double w, double h );
	static bool bound3DTriangleDisjoint( double px1, double py1, double pz1, double px2, double py2, double pz2, 
										double px3, double py3, double pz3,
									     double px0, double py0, double pz0, double w, double d, double h );

	static double squaredDistancePoint2Line( double px, double py, double x1, double y1, double x2, double y2 );
	static int relationLineCircle( double x1, double y1, double x2, double y2, 
								    double x0, double y0, double r );
	static int relationLineEllipse( double x1, double y1, double x2, double y2, 
								    double x0, double y0, double a, double b, double nx );
	static int relationLineNormalEllipse( double x1, double y1, double x2, double y2, double a, double b );
	static double squaredDistance3DPoint2Line( double px, double py, double pz,
			double x1, double y1, double z1, double x2, double y2, double z2 );
	static int relationLine3DSphere( double x1, double y1, double z1, double x2, double y2, double z2,
									  double x3, double y3, double z3, double r );
	static int relationLine3DNormalEllipsoid( double x1, double y1, double z1,  
													double x2, double y2, double z2,
													double a, double b, double c );
	static int relationLine3DEllipsoid( double x1, double y1, double z1,  
													double x2, double y2, double z2,
													double x0, double y0, double z0,
													double a, double b, double c, double nx, double ny );

	static int relationLine3DCone( double x1, double y1, double z1, double x2, double y2, double z2,
								double x0, double y0, double z0,
								double a, double c, double nx, double ny );
	static int relationLine3DNormalCone( double x0, double y0, double z0,  double x1, double y1, double z1,
										double a, double c );

	static int relationLine3DCylinder( double x1, double y1, double z1, double x2, double y2, double z2,
								double x0, double y0, double z0,
								double a, double b, double c, double nx, double ny );
	static int relationLine3DNormalCylinder( double x0, double y0, double z0,  double x1, double y1, double z1,
										double a, double b, double c );


	static bool point3DWithinRectangle2D(  double px, double py, double pz, 
											double x0, double y0, double z0,
	 										double a, double b, double nx, double ny, bool strict );


	static void cornersOfRectangle( double w, double h, JagPoint2D p[] );
	static void cornersOfRectangle3D( double w, double h, JagPoint3D p[] );
	static void cornersOfBox( double w, double d, double h, JagPoint3D p[] );
	static void edgesOfRectangle( double w, double h, JagLine2D line[] );
	static void edgesOfRectangle3D( double w, double h, JagLine3D line[] );
	static void edgesOfBox( double w, double d, double h, JagLine3D line[] );
	static void edgesOfTriangle( double x1, double y1, double x2, double y2, double x3, double y3, JagLine2D line[] );
	static void edgesOfTriangle3D( double x1, double y1, double z1, double x2, double y2, double z2, 
									double x3, double y3, double z3, JagLine3D line[] );
	static void surfacesOfBox(double w, double d, double h, JagRectangle3D rect[] );
	static void triangleSurfacesOfBox(double w, double d, double h, JagTriangle3D tri[] );
	static void pointsOfTriangle3D( double x1, double y1, double z1, double x2, double y2, double z2, 
									double x3, double y3, double z3, JagPoint3D point[] );


	static void transform3DEdgesLocal2Global( double x0, double y0, double z0, double nx0, double ny0, 
											  int num, JagPoint3D[] );
	static void transform2DEdgesLocal2Global( double x0, double y0, double nx0, int num, JagPoint2D[] );

	static void transform3DLinesLocal2Global( double x0, double y0, double z0, double nx0, double ny0, 
											 int num, JagLine3D line[] );
	static void transform3DLinesGlobal2Local( double x0, double y0, double z0, double nx0, double ny0, 
											 int num, JagLine3D line[] );
	static void transform2DLinesLocal2Global( double x0, double y0, double nx0, int num, JagLine2D line[] );
	static void transform2DLinesGlobal2Local( double x0, double y0, double nx0, int num, JagLine2D line[] );

	static bool line2DIntersectNormalRectangle( const JagLine2D &line, double w, double h );
	static bool line2DIntersectRectangle( const JagLine2D &line, double x0, double y0, double w, double h, double nx );
	static bool line3DIntersectNormalRectangle( const JagLine3D &line, double w, double h );
	static bool line3DIntersectRectangle3D( const JagLine3D &line, 
										    double x0, double y0, double z0, double w, double h, double nx, double ny );
	static bool line3DIntersectRectangle3D( const JagLine3D &line, const JagRectangle3D &rect );

	static bool line3DIntersectEllipse3D( const JagLine3D &line, 
									      double x0, double y0, double w, double d, double h, double nx, double ny );
	static bool line3DIntersectNormalEllipse( const JagLine3D &line, double w, double h );
	static bool line3DIntersectLine3D( double x1, double y1, double z1, double x2, double y2, double z2,
										double x3, double y3, double z3, double x4, double y4, double z4 );
	static bool line3DIntersectLine3D( const JagLine3D &line1, const JagLine3D &line2 );


	static double distanceFromPoint3DToPlane(  double x, double y, double z, 
									 double x0, double y0, double z0, double nx0, double ny0 );

	static void triangle3DNormal( double x1, double y1, double z1, double x2, double y2, double z2,
								  double x3, double y3, double z3, double &nx, double &ny );
	static void triangle3DABCD( double x1, double y1, double z1, double x2, double y2, double z2,
								  double x3, double y3, double z3, double &A, double &B, double &C, double &D );

	static double distancePoint3DToPlane( double x, double y, double z, double A, double B, double C, double D );
	static double distancePoint3DToTriangle3D( double x, double y, double z, 
									double x1, double y1, double z1, double x2, double y2, double z2,
									double x3, double y3, double z3 );

	static void planeABCDFromNormal( double x0, double y0, double z0, double nx, double ny, 
								    double &A, double &B, double &C, double &D );
			
	static bool planeIntersectNormalEllipsoid( double A, double B, double C, double D,
										  double a,  double b,  double c );
	static bool planeIntersectNormalCone( double A, double B, double C, double D,
										  double R,  double h );
	static void getCoordAvg( const AbaxDataString& colType, const JagStrSplit &sp, double &x, double &y, double &z, 
							double &Rx, double &Ry, double &Rz );

	static void triangleRegion( double x1, double y1, double x2, double y2, double x3, double y3,
								double &x0, double &y0, double &Rx, double &Ry );
	static void triangle3DRegion( double x1, double y1, double z1, 
								  double x2, double y2, double z2,
								  double x3, double y3, double z3,
								double &x0, double &y0, double &z0, double &Rx, double &Ry, double &Rz );

	static void boundingBoxRegion( const AbaxDataString &bbxstr, double &bbx, double &bby, double &brx, double &bry );
	static void boundingBox3DRegion( const AbaxDataString &bbxstr, double &bbx, double &bby, double &bbz, 
									double &brx, double &bry, double &brz );
	static void prepareKMP( const JagStrSplit &sp, int start, int M, int *lps );
	static int KMPPointsWithin( const JagStrSplit &sp1, int start1, const JagStrSplit &sp2, int start2 );
	static short orientation(double x1, double y1, double x2, double y2, double x3, double y3 );
	static short orientation(double x1, double y1, double z1, double x2, double y2, double z2, 
							 double x3, double y3, double z3 );
    static void getPolygonBound( const AbaxDataString &mk, const JagStrSplit &sp, double &bbx, double &bby, 
								  double &rx, double &ry );
    static void getLineStringBound( const AbaxDataString &mk, const JagStrSplit &sp, double &bbx, double &bby, 
								  double &rx, double &ry );

	static double dotProduct( const JagPoint2D &p1, const JagPoint2D &p2 ); 
	static double dotProduct( const JagPoint3D &p1, const JagPoint3D &p2 ); 
	static double dotProduct( double x1, double y1, double x2, double y2 );
	static double dotProduct( double x1, double y1, double z1, double x2, double y2, double z2 );
	static void crossProduct( double x1, double y1, double x2, double y2, double &x, double &y );
	static void crossProduct( double x1, double y1, double z1, double x2, double y2, double z2,
							  double &x, double &y, double &z );
	static void minusVector( const JagPoint3D &v1, const JagPoint3D &v2, JagPoint3D &pt );

	static int line3DIntersectTriangle3D( const JagLine3D& line3d, const JagPoint3D &p1, 
										  const JagPoint3D &p2, const JagPoint3D &p3,
										  JagPoint3D &atPoint );

	static int getBarycentric( const JagPoint3D &pt, const JagPoint3D &a, const JagPoint3D &b, const JagPoint3D &c,
							   double &u, double &v, double &w);

	static bool point3DWithinTriangle3D( double x, double y, double z, 
										 const JagPoint3D &p1, const JagPoint3D &p2, const JagPoint3D &p3 );


    static double meterToLon( int srid, double meter, double lon, double lat);
    static double meterToLat( int srid, double meter, double lon, double lat);
	static double computePolygonArea( const JagVector<std::pair<double,double>> &vec );
	static bool lineStringAverage( const AbaxDataString &mk, const JagStrSplit &sp, double &x, double &y );
	static bool lineString3DAverage( const AbaxDataString &mk, const JagStrSplit &sp, double &x, double &y, double &z );




};  // end of class JagGeo



#include <JagSortLinePoints.h>
#endif
