#ifndef _jag_cgal_h_
#define _jag_cgal_h_

#include <abax.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/geometries.hpp>
#include <boost/geometry/algorithms/perimeter.hpp>
#include <boost/range/algorithm.hpp>

//#define CGAL_HEADER_ONLY 1
#include <CGAL/Exact_predicates_inexact_constructions_kernel.h>
#include <CGAL/convex_hull_2.h>
#include <CGAL/Surface_mesh.h>
#include <CGAL/boost/graph/graph_traits_Surface_mesh.h>
#include <CGAL/boost/graph/Euler_operations.h>
#include <CGAL/Simple_cartesian.h>
#include <CGAL/Surface_mesh_simplification/edge_collapse.h>
#include <CGAL/Surface_mesh_simplification/Policies/Edge_collapse/Count_ratio_stop_predicate.h>
#include <CGAL/Memory_sizer.h>
#include <CGAL/convex_hull_3.h>
#include <CGAL/intersections.h>

#include <JagGeom.h>

typedef CGAL::Exact_predicates_inexact_constructions_kernel CGALKernel;
typedef CGALKernel::Point_2 CGALPoint2D;
typedef CGALKernel::Point_3 CGALPoint3D;
typedef CGAL::Surface_mesh<CGALKernel::Point_3> CGALSurfaceMesh;

typedef boost::geometry::model::d2::point_xy<double> BoostPoint2D;
typedef boost::geometry::model::linestring<BoostPoint2D> BoostLineString2D;
typedef boost::geometry::model::polygon<BoostPoint2D,false> BoostPolygon2D; // counter-clock-wise
typedef boost::geometry::model::polygon<BoostPoint2D,true> BoostPolygon2DCW; // clock-wise
typedef boost::geometry::ring_type<BoostPolygon2D>::type BoostRing2D; // counter-clock-wise
typedef boost::geometry::ring_type<BoostPolygon2DCW>::type BoostRing2DCW; // clock-wise

//typedef boost::geometry::interior_type<BoostPolygon2DCW>::type BoostInnerRing2D;

//typedef boost::geometry::ring_type<BoostPolygon2D,false>::type BoostRing2DCCW; // counter-clock-wise
//typedef boost::geometry::ring_type<BoostPoint2D>::type BoostRing2D; // clock-wise
//typedef boost::geometry::ring_type<BoostPoint2D,false>::type BoostRing2DCCW; // counter-clock-wise

//typedef bg::model::ring<DPoint, false> DRing;
//typedef bg::model::ring<BoostPoint2D, true>  BoostRing2D;
//typedef bg::model::ring<BoostPoint2D, false> BoostRing2DCCW;


typedef boost::geometry::strategy::buffer::distance_symmetric<double> JagDistanceSymmetric;
typedef boost::geometry::strategy::buffer::distance_asymmetric<double> JagDistanceASymmetric;
typedef boost::geometry::strategy::buffer::side_straight JagSideStraight;
typedef boost::geometry::strategy::buffer::join_round JagJoinRound;
typedef boost::geometry::strategy::buffer::join_miter JagJoinMiter;
typedef boost::geometry::strategy::buffer::end_round JagEndRound;
typedef boost::geometry::strategy::buffer::end_flat JagEndFlat;
typedef boost::geometry::strategy::buffer::point_circle JagPointCircle;
typedef boost::geometry::strategy::buffer::point_square JagPointSquare;


class JagStrategy
{
  public:
  	JagStrategy( const AbaxDataString &nm, double a1, double a2 );
  	~JagStrategy();
	void  *ptr;
  	AbaxDataString name;
	short  t;
  protected:
	double f1, f2;
};


class JagCGAL
{
  public:
    static void getConvexHull2DStr( const JagLineString &line, const AbaxDataString &hdr, const AbaxDataString &bbox, AbaxDataString &value );
    static void getConvexHull2D( const JagLineString &line, JagLineString &hull );

    static void getConvexHull3DStr( const JagLineString &line, const AbaxDataString &hdr, const AbaxDataString &bbox, AbaxDataString &value );
    static void getConvexHull3D( const JagLineString &line, JagLineString &hull );

    static bool getBufferLineString2DStr( const JagLineString &line, int srid, const AbaxDataString &arg, AbaxDataString &value );
    static bool getBufferMultiPoint2DStr( const JagLineString &line, int srid, const AbaxDataString &arg, AbaxDataString &value );
    static bool getBufferPolygon2DStr( const JagPolygon &pgon, int srid, const AbaxDataString &arg, AbaxDataString &value );
    static bool getBufferMultiLineString2DStr( const JagPolygon &pgon, int srid, const AbaxDataString &arg, AbaxDataString &value );
    static bool getBufferMultiPolygon2DStr( const JagVector<JagPolygon> &pgvec, int srid, const AbaxDataString &arg, AbaxDataString &value );

    static bool getIsSimpleLineString2DStr( const JagLineString &line );
    static bool getIsSimpleMultiLineString2DStr( const JagPolygon &pgon );
    static bool getIsSimplePolygon2DStr( const JagPolygon &pgon );
    static bool getIsSimpleMultiPolygon2DStr( const JagVector<JagPolygon> &pgvec );

    static bool getIsValidLineString2DStr( const JagLineString &line );
    static bool getIsValidMultiLineString2DStr( const JagPolygon &pgon );
    static bool getIsValidPolygon2DStr( const JagPolygon &pgon );
    static bool getIsValidMultiPolygon2DStr( const JagVector<JagPolygon> &pgvec );
    static bool getIsValidMultiPoint2DStr( const JagLineString &line );

    static bool getIsRingLineString2DStr( const JagLineString &line );

	template <class TGeo>
    static bool getBuffer2D( const TGeo &obj, const AbaxDataString &arg, JagVector<JagPolygon> &pgvec );

	static bool get2DStrFromMultiPolygon( const JagVector<JagPolygon> &pgvec, int srid, AbaxDataString &value );
    static void getRingStr( const JagLineString &line, const AbaxDataString &hdr, const AbaxDataString &bbox, bool is3D, AbaxDataString &value );
    static void getOuterRingsStr( const JagVector<JagPolygon> &pgvec, const AbaxDataString &hdr, const AbaxDataString &bbox, 
									bool is3D, AbaxDataString &value );
    static void getInnerRingsStr( const JagVector<JagPolygon> &pgvec, const AbaxDataString &hdr, const AbaxDataString &bbox, 
									bool is3D, AbaxDataString &value );
    static void getPolygonNStr( const JagVector<JagPolygon> &pgvec, const AbaxDataString &hdr, const AbaxDataString &bbox, 
							   bool is3D, int N, AbaxDataString &value );
    static void getUniqueStr( const JagStrSplit &sp, const AbaxDataString &hdr, const AbaxDataString &bbox, AbaxDataString &value );
	static int unionOfTwoPolygons( const JagStrSplit &sp1, const JagStrSplit &sp2, AbaxDataString &wkt );
	static int unionOfPolygonAndMultiPolygons( const JagStrSplit &sp1, const JagStrSplit &sp2, AbaxDataString &wkt );
	static bool hasIntersection( const JagLine2D &line1, const JagLine2D &line2, JagVector<JagPoint2D> &res ); 
	static bool hasIntersection( const JagLine3D &line1, const JagLine3D &line2, JagVector<JagPoint3D> &res ); 
	static void split2DSPToVector( const JagStrSplit &sp, JagVector<JagPoint2D> &vec1 );
	static void split3DSPToVector( const JagStrSplit &sp, JagVector<JagPoint3D> &vec1 );
	static int  getTwoPolygonIntersection( const JagPolygon &pgon1, const JagPolygon &pgon2, JagVector<JagPolygon> &vec );
	static bool convertPolygonJ2B( const JagPolygon &pgon1, BoostPolygon2D &bgon );


  protected:
  	static bool createStrategies( JagStrategy *sptr[], const AbaxDataString &arg );
  	static void destroyStrategies( JagStrategy *sptr[] );


};

class JagIntersectionVisitor2D
{
  public:
    typedef void result_type;
    JagIntersectionVisitor2D( JagVector<JagPoint2D> &res ) : _res( res ) { }

    void operator()(const CGALKernel::Point_2& p) const
    {
		_res.append(JagPoint2D(p.x(), p.y()) );
    }

    void operator()(const CGALKernel::Segment_2& seg) const
    {
		CGALPoint2D s = seg.source();
		CGALPoint2D t = seg.target();
		_res.append(JagPoint2D(s.x(), s.y() ));
		_res.append(JagPoint2D(t.x(), t.y() ));
    }

	JagVector<JagPoint2D> &_res;
};


class JagIntersectionVisitor3D
{
  public:
    typedef void result_type;
    JagIntersectionVisitor3D( JagVector<JagPoint3D> &res ) : _res( res ) { }

    void operator()(const CGALKernel::Point_3& p) const
    {
		_res.append(JagPoint3D(p.x(), p.y(), p.z() ) );
    }

    void operator()(const CGALKernel::Segment_3& seg) const
    {
		CGALPoint3D s = seg.source();
		CGALPoint3D t = seg.target();
		_res.append(JagPoint3D(s.x(), s.y(), s.z() ));
		_res.append(JagPoint3D(t.x(), t.y(), t.z() ));
    }

	JagVector<JagPoint3D> &_res;
};


#endif
