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


class JagCGAL
{
  public:
    static void getConvexHull2DStr( const JagLineString &line, const AbaxDataString &hdr, const AbaxDataString &bbox, AbaxDataString &value );
    static void getConvexHull2D( const JagLineString &line, JagLineString &hull );

    static void getConvexHull3DStr( const JagLineString &line, const AbaxDataString &hdr, const AbaxDataString &bbox, AbaxDataString &value );
    static void getConvexHull3D( const JagLineString &line, JagLineString &hull );

  protected:

};


#endif
