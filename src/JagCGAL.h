#ifndef _jag_cgal_h_
#define _jag_cgal_h_

#include <abax.h>

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
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/geometries.hpp>
#include <boost/geometry/algorithms/perimeter.hpp>


#include <JagGeom.h>

typedef CGAL::Exact_predicates_inexact_constructions_kernel CGALKernel;
typedef CGALKernel::Point_2 CGALKernelPoint_2;
typedef std::vector<CGALKernelPoint_2> CGALKernelPoints2D;

typedef CGALKernel::Point_3 CGALKernelPoint_3;
typedef std::vector<CGALKernelPoint_3> CGALKernelPoints3D;

typedef CGAL::Surface_mesh<CGALKernelPoint_3> CGALSurfaceMesh;

typedef CGAL::Simple_cartesian<double> CartKernel;
typedef CartKernel::Point_2 CartKernelPoint2;
typedef CartKernel::Point_3 CartKernelPoint3;
//typedef CartKernel::Segment_2 CartKernelSegment2;

typedef boost::geometry::model::d2::point_xy<double> BoostPoint2D;
typedef boost::geometry::model::linestring<BoostPoint2D> BoostLineString2D;
typedef boost::geometry::model::polygon<BoostPoint2D,false> BoostPolygon2D;
typedef boost::geometry::ring_type<BoostPolygon2D>::type BoostRingType;


typedef boost::geometry::strategy::buffer::distance_symmetric<double> JagDistanceSymmetric;
typedef boost::geometry::strategy::buffer::distance_asymmetric<double> JagDistanceASymmetric;
typedef boost::geometry::strategy::buffer::side_straight JagSideStraight;
typedef boost::geometry::strategy::buffer::join_round JagJoinRound;
typedef boost::geometry::strategy::buffer::join_miter JagJoinMiter;
typedef boost::geometry::strategy::buffer::end_round JagEndRound;
typedef boost::geometry::strategy::buffer::end_flat JagEndFlat;
typedef  boost::geometry::strategy::buffer::point_circle JagPointCircle;
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
    static void getOuterRingStr( const JagLineString &line, const AbaxDataString &hdr, const AbaxDataString &bbox, bool is3D, AbaxDataString &value );

  protected:
  	static bool createStrategies( JagStrategy *sptr[], const AbaxDataString &arg );
  	static void destroyStrategies( JagStrategy *sptr[] );


};


#endif
