#include <JagGlobalDef.h>
#include <JagCGAL.h>

/***
 * typedef CGAL::Exact_predicates_inexact_constructions_kernel CGALKernel;
 * typedef CGALKernel::Point_2 CGALKernelPoint_2;
 * typedef std::vector<Point_2> CGALKernelPoints;
 *
 * typedef CGAL::Simple_cartesian<double> CartKernel;
 * typedef CartKernel::Point_2 CartKernelPoint2;
 * typedef CartKernel::Segment_2 CartKernelSegment2;
 *
***/

void JagCGAL::getConvexHull2DStr( const JagLineString &line, const AbaxDataString &hdr, const AbaxDataString &bbox, AbaxDataString &value )
{
	JagLineString hull;
	getConvexHull2D( line, hull );
	const JagVector<JagPoint> &point = hull.point;
	AbaxDataString sx, sy;

	if ( bbox.size() < 1 ) {
		double xmin, ymin, xmax, ymax;
		AbaxDataString s1, s2, s3, s4;
		hull.bbox2D( xmin, ymin, xmax, ymax );
		AbaxDataString newbbox;
		s1 = doubleToStr( xmin ).trimEndZeros();
		s2 = doubleToStr( ymin ).trimEndZeros();
		s3 = doubleToStr( xmax ).trimEndZeros();
		s4 = doubleToStr( ymax ).trimEndZeros();
		newbbox = s1 + ":" + s2 + ":" + s3 + ":" + s4;
		value = hdr + " " + newbbox;
	} else {
		value = hdr + " " + bbox;
	}

	for ( int i = 0; i <  point.size(); ++i ) {
		sx = point[i].x; sx.trimEndZeros();
		sy = point[i].y; sy.trimEndZeros();
		value += AbaxDataString(" ") + sx + ":" + sy;
	}
}

void JagCGAL::getConvexHull2D( const JagLineString &line, JagLineString &hull )
{
	CGALKernelPoints vpoints, vresult;
	const JagVector<JagPoint> &point = line.point;
	for ( int i = 0; i <  point.size(); ++i ) {
		vpoints.push_back(  CGALKernelPoint_2( jagatof(point[i].x), jagatof(point[i].y) ));
	}

	CGAL::convex_hull_2( vpoints.begin(), vpoints.end(), std::back_inserter(vresult) );
	for( int i = 0; i < vresult.size(); i++) {
		hull.add( vresult[i].x(), vresult[i].y() );
	}
}

