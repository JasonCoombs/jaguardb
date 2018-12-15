#ifndef _jag_cgal_h_
#define _jag_cgal_h_

#include <abax.h>
#include <JagGeom.h>

#define CGAL_HEADER_ONLY 1
#include <CGAL/Exact_predicates_inexact_constructions_kernel.h>
#include <CGAL/convex_hull_2.h>

typedef CGAL::Exact_predicates_inexact_constructions_kernel CGALKernel;
typedef CGALKernel::Point_2 CGALKernelPoint_2;
typedef std::vector<CGALKernelPoint_2> CGALKernelPoints;

typedef CGAL::Simple_cartesian<double> CartKernel;
typedef CartKernel::Point_2 CartKernelPoint2;
typedef CartKernel::Segment_2 CartKernelSegment2;

class JagCGAL
{
  public:
    static void getConvexHull2DStr( const JagLineString &line, const AbaxDataString &hdr, const AbaxDataString &bbox, AbaxDataString &value );
    static void getConvexHull2D( const JagLineString &line, JagLineString &hull );


  protected:

};


#endif
