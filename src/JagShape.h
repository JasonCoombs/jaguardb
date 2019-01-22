#ifndef _jag_shape_h_
#define _jag_shape_h_

#include <math.h>

#define jagmin(a, b) ( (a) < (b) ? (a) : (b))
#define jagmax(a, b) ( (a) > (b) ? (a) : (b))
#define jagmin3(a, b, c) jagmin(jagmin(a, b),(c))
#define jagmax3(a, b, c) jagmax(jagmax(a, b),(c))
#define jagsq2(a) ( (a)*(a) )
#define jagIsPosZero(a) (((a)<ZERO) ? 1 : 0)
#define jagIsZero(a) (( fabs(a)<ZERO) ? 1 : 0)
#define JAG_PI 3.141592653589793


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
	 int operator< ( const JagPoint2D &p2 ) const 
	 { 
	 	return (x < p2.x && y < p2.y );
	 }
	 int operator<= ( const JagPoint2D &p2 ) const 
	 { 
	 	return (x <= p2.x && y <= p2.y );
	 }
	 int operator>= ( const JagPoint2D &p2 ) const 
	 { 
	 	return (x >= p2.x && y >= p2.y );
	 }
	 int operator== ( const JagPoint2D &p2 ) const 
	 { 
	 	return ( fabs(x-p2.x) < 0.0000001 && fabs(y - p2.y) < 0.0000001 );
	 }
	 int operator!= ( const JagPoint2D &p2 ) const 
	 { 
	 	return ( fabs(x-p2.x) > 0.0000001 || fabs(y - p2.y) > 0.0000001 );
	 }

};


class JagSquare2D
{
  public:
  	JagSquare2D(){ srid = 0; }
  	JagSquare2D( double inx, double iny, double ina, double innx, int srid=0 );
  	void init( double inx, double iny, double ina, double innx, int srid );
  	JagSquare2D( const JagStrSplit &sp, int srid=0 );
	double x0, y0, a, nx; 
	int srid;
	JagPoint2D point[4];  // counter-clockwise polygon points
};

class JagRectangle2D
{
  public:
  	JagRectangle2D(){ srid=0;};
  	JagRectangle2D( double inx, double iny, double ina, double inb, double innx, int srid=0 );
  	JagRectangle2D( const JagStrSplit &sp, int srid=0 );
  	void init( double inx, double iny, double ina, double inb, double innx, int srid );
	static void setPoint( double x0, double y0, double a, double b, double nx, JagPoint2D point[] );
	double x0, y0, a, b, nx; 
	int srid;
	JagPoint2D point[4];  // counter-clockwise polygon points
};

class JagCircle2D
{
  public:
  	JagCircle2D(){ srid=0;};
  	JagCircle2D( double inx, double iny, double inr, int srid=0 );
  	void init( double inx, double iny, double inr, int srid );
  	JagCircle2D( const JagStrSplit &sp, int srid=0 );
	int srid;
	double x0, y0, r;
};

class JagEllipse2D
{
  public:
  	JagEllipse2D(){ srid = 0;};
  	JagEllipse2D( double inx, double iny, double ina, double inb, double innx, int srid=0 );
  	void init( double inx, double iny, double ina, double inb, double innx, int srid );
  	JagEllipse2D( const JagStrSplit &sp, int srid=0 );
	double x0, y0, a, b, nx; 
	int srid;
};

class JagTriangle2D
{
  public:
  	JagTriangle2D(){ srid=0;};
  	JagTriangle2D( double inx1, double iny1, double inx2, double iny2, double inx3, double iny3, int srid=0 );
  	void init( double inx1, double iny1, double inx2, double iny2, double inx3, double iny3, int srid );
  	JagTriangle2D( const JagStrSplit &sp, int srid=0 );
	double x1,y1, x2,y2, x3,y3;
	int srid;
};



//////////// from old jaggeo.h ///////////
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
	 AbaxDataString hashString() const;
	 AbaxDataString str3D() const { return d2s(x) + ":" + d2s(y) + ":" + d2s(z); }
	 AbaxDataString str2D() const { return d2s(x) + ":" + d2s(y); }
     double x;
	 double y;
	 double z;
	 int operator< ( const JagPoint3D &p2 ) const 
	 { 
	 	return (x < p2.x && y < p2.y && z < p2.z );
	 }
	 int operator<= ( const JagPoint3D &p2 ) const 
	 { 
	 	return (x <= p2.x && y <= p2.y && z <= p2.z );
	 }
	 int operator>= ( const JagPoint3D &p2 ) const 
	 { 
	 	return (x >= p2.x && y >= p2.y && z >= p2.z );
	 }
	 int operator== ( const JagPoint3D &p2 ) const 
	 { 
	 	return ( fabs(x-p2.x) < 0.0000001 && fabs(y - p2.y) < 0.0000001 && fabs(z - p2.z ) < 0.0000001 );
	 }
	 int operator!= ( const JagPoint3D &p2 ) const 
	 { 
	 	return ( fabs(x-p2.x) > 0.0000001 || fabs(y - p2.y) > 0.0000001 || fabs(z - p2.z ) > 0.0000001 );
	 }
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
		bool equal2D(const JagPoint &JagPoint ) const;
		bool equal3D(const JagPoint &JagPoint ) const;
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
		void add( double x, double y, double z=0.0 );
		void print() const { point.print(); }
		void center2D( double &cx, double &cy, bool dropLast=false ) const;
		void center3D( double &cx, double &cy, double &cz, bool dropLast=false ) const;
		void bbox2D( double &xmin, double &ymin, double &xmax, double &ymax ) const;
		void bbox3D( double &xmin, double &ymin, double &zmin, double &xmax, double &ymax, double &zmax ) const;
		const JagPoint3D& operator[](int i ) const { return point[i]; }
		double lineLength( bool removeLast, bool is3D );

		JagVector<JagPoint3D> point;
};

class JagLineString
{
    public:
		JagLineString() {};
		JagLineString& operator=( const JagLineString& L2 ) { point = L2.point; return *this; }
		JagLineString& operator=( const JagLineString3D& L2 );
		JagLineString& copyFrom( const JagLineString3D& L2, bool removeLast = false );
		JagLineString& appendFrom( const JagLineString3D& L2, bool removeLast = false );
		JagLineString( const JagLineString& L2 ) { point = L2.point; }
		void init() { point.clean(); };
		abaxint size() const { return point.size(); }
		void add( const JagPoint2D &p );
		void add( const JagPoint3D &p );
		void add( double x, double y );
		void add( double x, double y, double z );
		void print() const { point.print(); }
		void center2D( double &cx, double &cy, bool dropLast=false ) const;
		void center3D( double &cx, double &cy, double &cz, bool dropLast=false ) const;
		void bbox2D( double &xmin, double &ymin, double &xmax, double &ymax ) const;
		void bbox3D( double &xmin, double &ymin, double &zmin, double &xmax, double &ymax, double &zmax ) const;
		const JagPoint& operator[](int i ) const { return point[i]; }
		double lineLength( bool removeLast, bool is3D );

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
		bool bbox2D( double &xmin, double &ymin, double &xmax, double &ymax ) const;
		bool bbox3D( double &xmin, double &ymin, double &zmin, double &xmax, double &ymax, double &zmax ) const;
		JagVector<JagLineString3D> linestr;
		void print() const { linestr.print(); }
		double lineLength( bool removeLast, bool is3D );
		void toWKT( bool is3D, bool hasHdr, const AbaxDataString &objname, AbaxDataString &str ) const;
		void toJAG( bool is3D, bool hasHdr, int srid, AbaxDataString &str ) const;

		JagPolygon( const JagSquare2D &sq );
		JagPolygon( const JagRectangle2D &rect );
		JagPolygon( const JagCircle2D &cir, int samples = 100 );
		JagPolygon( const JagEllipse2D &e, int samples = 100 );
		JagPolygon( const JagTriangle2D &t );
		
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


#endif
