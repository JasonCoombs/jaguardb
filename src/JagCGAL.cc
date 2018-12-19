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
	CGALKernelPoints2D vpoints, vresult;
	const JagVector<JagPoint> &point = line.point;
	for ( int i = 0; i <  point.size(); ++i ) {
		vpoints.push_back(  CGALKernelPoint_2( jagatof(point[i].x), jagatof(point[i].y) ));
	}

	CGAL::convex_hull_2( vpoints.begin(), vpoints.end(), std::back_inserter(vresult) );
	for( int i = 0; i < vresult.size(); i++) {
		hull.add( vresult[i].x(), vresult[i].y() );
	}
}

void JagCGAL::getConvexHull3D( const JagLineString &line, JagLineString &hull )
{
	prt(("s3382 getConvexHull3D line.size=%d\n", line.size() ));
	CGALKernelPoints3D vpoints;
	const JagVector<JagPoint> &point = line.point;
	for ( int i = 0; i <  point.size(); ++i ) {
		vpoints.push_back(  CGALKernelPoint_3( jagatof(point[i].x), jagatof(point[i].y), jagatof(point[i].z) ));
	}

	CGALSurfaceMesh sm;
	CGAL::convex_hull_3( vpoints.begin(), vpoints.end(), sm );

	// CGALSurfaceMesh::Vertex_range vrange = sm.vertices();
	BOOST_FOREACH( CGALSurfaceMesh::Vertex_index vi, sm.vertices()){
	    //std::cout << vi << std::endl;
	    //std::cout << sm.point(vi) << std::endl;
	    hull.add( sm.point(vi).x(), sm.point(vi).y(), sm.point(vi).z() );
    }
}

void JagCGAL::getConvexHull3DStr( const JagLineString &line, const AbaxDataString &hdr, const AbaxDataString &bbox, AbaxDataString &value )
{
	JagLineString hull;
	getConvexHull3D( line, hull );
	const JagVector<JagPoint> &point = hull.point;
	AbaxDataString sx, sy, sz;

	if ( bbox.size() < 1 ) {
		double xmin, ymin, zmin, xmax, ymax, zmax;
		AbaxDataString s1, s2, s3, s4, s5, s6;
		hull.bbox3D( xmin, ymin, zmin, xmax, ymax, zmax );
		AbaxDataString newbbox;
		s1 = doubleToStr( xmin ).trimEndZeros();
		s2 = doubleToStr( ymin ).trimEndZeros();
		s3 = doubleToStr( zmin ).trimEndZeros();
		s4 = doubleToStr( xmax ).trimEndZeros();
		s5 = doubleToStr( ymax ).trimEndZeros();
		s6 = doubleToStr( zmax ).trimEndZeros();
		newbbox = s1 + ":" + s2 + ":" + s3 + ":" + s4 + ":" + s5 + ":" + s6;
		value = hdr + " " + newbbox;
	} else {
		value = hdr + " " + bbox;
	}

	for ( int i = 0; i <  point.size(); ++i ) {
		sx = point[i].x; sx.trimEndZeros();
		sy = point[i].y; sy.trimEndZeros();
		sz = point[i].z; sz.trimEndZeros();
		value += AbaxDataString(" ") + sx + ":" + sy + ":" + sz;
	}
}
