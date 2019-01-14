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
	//prt(("s3382 getConvexHull3D line.size=%d\n", line.size() ));
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

/***
template<typename GeometryIn, typename MultiPolygon, typename DistanceStrategy, typename SideStrategy,
         typename JoinStrategy, typename EndStrategy, typename PointStrategy>
void buffer(GeometryIn const & geometry_in, MultiPolygon & geometry_out, DistanceStrategy const & distance_strategy,
            SideStrategy const & side_strategy, JoinStrategy const & join_strategy, EndStrategy const & end_strategy,
            PointStrategy const & point_strategy)

distance_strategy: symmetric or assymetric, and positive or negative
	JagDistanceSymmetric<double> distance_strategy(0.5);
	JagDistanceASymmetric distance_strategy(1.0, 0.5); // left/right side distance


side_strategy : JagSideStraight side_strategy;
	JagSideStraight side_strategy;


join_strategy : rounded or sharp
	JagJoinRound join_strategy(72);  // # of points
	JagJoinMiter join_strategy;

end_strategy : rounded or flat
	JagEndRound end_strategy(36);  // # of points
	JagEndFlat end_strategy;

point_strategy : circular or square
	JagPointSquare point_strategy;
	JagPointCircle point_strategy(360);  // # of points
***/

//bool JagCGAL::getBuffer2D( const JagLineString &line, const AbaxDataString &arg, bool isMultiPoint, JagVector<JagPolygon> &mpgon )

template <class TGeo>
bool JagCGAL::getBuffer2D( const TGeo &ls, const AbaxDataString &arg, JagVector<JagPolygon> &mpgon )
{
	bool rc;
	JagStrategy *sptr[5];
	rc = createStrategies( sptr, arg );
	if ( ! rc ) {
		prt(("s2981 createStrategies error\n" ));
		return false;
	}

	boost::geometry::model::multi_polygon<BoostPolygon2D> result;
	//boost::geometry::model::linestring<BoostPoint2D> ls;
	//TGeo ls;

	//line.print();
	/***  do this in caller
	for ( int i=0; i < line.size(); ++i ) {
		boost::geometry::append( ls, BoostPoint2D( jagatof(line.point[i].x), jagatof(line.point[i].y) ) );
		prt(("s1129 append i=%d x=%s y=%s\n", i, line.point[i].x, line.point[i].y ));
	}
	***/

	if ( sptr[0]->t == JAG_DIST_SYMMETRIC && sptr[2]->t == JAG_JOIN_ROUND && sptr[3]->t == JAG_END_ROUND && sptr[4]->t==JAG_POINT_CIRCLE ) {
		boost::geometry::buffer( ls, result, *( (JagDistanceSymmetric*)sptr[0]->ptr), 
							 *( (JagSideStraight*)sptr[1]->ptr), *( (JagJoinRound*)sptr[2]->ptr), 
							 *( (JagEndRound*)sptr[3]->ptr), *( (JagPointCircle*)sptr[4]->ptr) );
	} else if ( sptr[0]->t == JAG_DIST_ASYMMETRIC && sptr[2]->t == JAG_JOIN_ROUND && sptr[3]->t == JAG_END_ROUND && sptr[4]->t==JAG_POINT_CIRCLE ) {
		boost::geometry::buffer( ls, result, *( (JagDistanceASymmetric*)sptr[0]->ptr), 
							 *( (JagSideStraight*)sptr[1]->ptr), *( (JagJoinRound*)sptr[2]->ptr), 
							 *( (JagEndRound*)sptr[3]->ptr), *( (JagPointCircle*)sptr[4]->ptr) );
	} else if ( sptr[0]->t == JAG_DIST_SYMMETRIC && sptr[2]->t == JAG_JOIN_MITER && sptr[3]->t == JAG_END_ROUND && sptr[4]->t==JAG_POINT_CIRCLE ) {
		boost::geometry::buffer( ls, result, *( (JagDistanceSymmetric*)sptr[0]->ptr), 
							 *( (JagSideStraight*)sptr[1]->ptr), *( (JagJoinMiter*)sptr[2]->ptr), 
							 *( (JagEndRound*)sptr[3]->ptr), *( (JagPointCircle*)sptr[4]->ptr) );
	} else if ( sptr[0]->t == JAG_DIST_ASYMMETRIC && sptr[2]->t == JAG_JOIN_MITER && sptr[3]->t == JAG_END_ROUND && sptr[4]->t==JAG_POINT_CIRCLE ) {
		boost::geometry::buffer( ls, result, *( (JagDistanceASymmetric*)sptr[0]->ptr), 
							 *( (JagSideStraight*)sptr[1]->ptr), *( (JagJoinMiter*)sptr[2]->ptr), 
							 *( (JagEndRound*)sptr[3]->ptr), *( (JagPointCircle*)sptr[4]->ptr) );
	} else if ( sptr[0]->t == JAG_DIST_SYMMETRIC && sptr[2]->t == JAG_JOIN_ROUND && sptr[3]->t == JAG_END_FLAT && sptr[4]->t==JAG_POINT_CIRCLE ) {
		boost::geometry::buffer( ls, result, *( (JagDistanceSymmetric*)sptr[0]->ptr), 
							 *( (JagSideStraight*)sptr[1]->ptr), *( (JagJoinRound*)sptr[2]->ptr), 
							 *( (JagEndFlat*)sptr[3]->ptr), *( (JagPointCircle*)sptr[4]->ptr) );
	} else if ( sptr[0]->t == JAG_DIST_SYMMETRIC && sptr[2]->t == JAG_JOIN_MITER && sptr[3]->t == JAG_END_FLAT && sptr[4]->t==JAG_POINT_CIRCLE ) {
		boost::geometry::buffer( ls, result, *( (JagDistanceSymmetric*)sptr[0]->ptr), 
							 *( (JagSideStraight*)sptr[1]->ptr), *( (JagJoinMiter*)sptr[2]->ptr), 
							 *( (JagEndFlat*)sptr[3]->ptr), *( (JagPointCircle*)sptr[4]->ptr) );
	} else if ( sptr[0]->t == JAG_DIST_ASYMMETRIC && sptr[2]->t == JAG_JOIN_ROUND && sptr[3]->t == JAG_END_FLAT && sptr[4]->t==JAG_POINT_CIRCLE ) {
		boost::geometry::buffer( ls, result, *( (JagDistanceASymmetric*)sptr[0]->ptr), 
							 *( (JagSideStraight*)sptr[1]->ptr), *( (JagJoinRound*)sptr[2]->ptr), 
							 *( (JagEndFlat*)sptr[3]->ptr), *( (JagPointCircle*)sptr[4]->ptr) );
	} else if ( sptr[0]->t == JAG_DIST_ASYMMETRIC && sptr[2]->t == JAG_JOIN_MITER && sptr[3]->t == JAG_END_FLAT && sptr[4]->t==JAG_POINT_CIRCLE ) {
		boost::geometry::buffer( ls, result, *( (JagDistanceASymmetric*)sptr[0]->ptr), 
							 *( (JagSideStraight*)sptr[1]->ptr), *( (JagJoinMiter*)sptr[2]->ptr), 
							 *( (JagEndFlat*)sptr[3]->ptr), *( (JagPointCircle*)sptr[4]->ptr) );
	} else if ( sptr[0]->t == JAG_DIST_SYMMETRIC && sptr[2]->t == JAG_JOIN_ROUND && sptr[3]->t == JAG_END_ROUND && sptr[4]->t == JAG_POINT_SQUARE ) {
		boost::geometry::buffer( ls, result, *( (JagDistanceSymmetric*)sptr[0]->ptr), 
							 *( (JagSideStraight*)sptr[1]->ptr), *( (JagJoinRound*)sptr[2]->ptr), 
							 *( (JagEndRound*)sptr[3]->ptr), *( (JagPointSquare*)sptr[4]->ptr) );
	} else if ( sptr[0]->t == JAG_DIST_ASYMMETRIC && sptr[2]->t == JAG_JOIN_ROUND && sptr[3]->t == JAG_END_ROUND && sptr[4]->t == JAG_POINT_SQUARE ) {
		boost::geometry::buffer( ls, result, *( (JagDistanceASymmetric*)sptr[0]->ptr), 
							 *( (JagSideStraight*)sptr[1]->ptr), *( (JagJoinRound*)sptr[2]->ptr), 
							 *( (JagEndRound*)sptr[3]->ptr), *( (JagPointSquare*)sptr[4]->ptr) );
	} else if ( sptr[0]->t == JAG_DIST_SYMMETRIC && sptr[2]->t == JAG_JOIN_MITER && sptr[3]->t == JAG_END_ROUND && sptr[4]->t == JAG_POINT_SQUARE ) {
		boost::geometry::buffer( ls, result, *( (JagDistanceSymmetric*)sptr[0]->ptr), 
							 *( (JagSideStraight*)sptr[1]->ptr), *( (JagJoinMiter*)sptr[2]->ptr), 
							 *( (JagEndRound*)sptr[3]->ptr), *( (JagPointSquare*)sptr[4]->ptr) );
	} else if ( sptr[0]->t == JAG_DIST_ASYMMETRIC && sptr[2]->t == JAG_JOIN_MITER && sptr[3]->t == JAG_END_ROUND && sptr[4]->t == JAG_POINT_SQUARE ) {
		boost::geometry::buffer( ls, result, *( (JagDistanceASymmetric*)sptr[0]->ptr), 
							 *( (JagSideStraight*)sptr[1]->ptr), *( (JagJoinMiter*)sptr[2]->ptr), 
							 *( (JagEndRound*)sptr[3]->ptr), *( (JagPointSquare*)sptr[4]->ptr) );
	} else if ( sptr[0]->t == JAG_DIST_SYMMETRIC && sptr[2]->t == JAG_JOIN_ROUND && sptr[3]->t == JAG_END_FLAT && sptr[4]->t == JAG_POINT_SQUARE ) {
		boost::geometry::buffer( ls, result, *( (JagDistanceSymmetric*)sptr[0]->ptr), 
							 *( (JagSideStraight*)sptr[1]->ptr), *( (JagJoinRound*)sptr[2]->ptr), 
							 *( (JagEndFlat*)sptr[3]->ptr), *( (JagPointSquare*)sptr[4]->ptr) );
	} else if ( sptr[0]->t == JAG_DIST_SYMMETRIC && sptr[2]->t == JAG_JOIN_MITER && sptr[3]->t == JAG_END_FLAT && sptr[4]->t == JAG_POINT_SQUARE ) {
		boost::geometry::buffer( ls, result, *( (JagDistanceSymmetric*)sptr[0]->ptr), 
							 *( (JagSideStraight*)sptr[1]->ptr), *( (JagJoinMiter*)sptr[2]->ptr), 
							 *( (JagEndFlat*)sptr[3]->ptr), *( (JagPointSquare*)sptr[4]->ptr) );
	} else if ( sptr[0]->t == JAG_DIST_ASYMMETRIC && sptr[2]->t == JAG_JOIN_ROUND && sptr[3]->t == JAG_END_FLAT && sptr[4]->t == JAG_POINT_SQUARE ) {
		boost::geometry::buffer( ls, result, *( (JagDistanceASymmetric*)sptr[0]->ptr), 
							 *( (JagSideStraight*)sptr[1]->ptr), *( (JagJoinRound*)sptr[2]->ptr), 
							 *( (JagEndFlat*)sptr[3]->ptr), *( (JagPointSquare*)sptr[4]->ptr) );
	} else if ( sptr[0]->t == JAG_DIST_ASYMMETRIC && sptr[2]->t == JAG_JOIN_MITER && sptr[3]->t == JAG_END_FLAT && sptr[4]->t == JAG_POINT_SQUARE ) {
		boost::geometry::buffer( ls, result, *( (JagDistanceASymmetric*)sptr[0]->ptr), 
							 *( (JagSideStraight*)sptr[1]->ptr), *( (JagJoinMiter*)sptr[2]->ptr), 
							 *( (JagEndFlat*)sptr[3]->ptr), *( (JagPointSquare*)sptr[4]->ptr) );
	} else {
		prt(("s8233 no match\n" ));
	}
	
	prt(("s9202 done buffer() result.size=%d\n", result.size() ));
	boost::geometry::model::multi_polygon<BoostPolygon2D>::iterator iter; 
	for ( iter = result.begin(); iter != result.end(); ++iter ) {
		prt(("s8272 iter :\n" ));
		//std::cout << *iter << std::endl;
		const BoostRingType &ring = iter->outer();
		JagLineString3D lstr;
		for ( auto it = boost::begin(ring); it != boost::end(ring); ++it ) {
			lstr.add( it->x(), it->y() );
			//prt(("s3772 it:\n" ));
			//std::cout << *it << std::endl;
		}
		JagPolygon pgon;
		pgon.linestr.append( lstr );
		mpgon.append( pgon );
	}

	destroyStrategies( sptr );
	return true;
}

bool JagCGAL::getBufferLineString2DStr( const JagLineString &line, int srid, const AbaxDataString &arg, AbaxDataString &value )
{
	JagVector<JagPolygon> pgvec;
	// bool rc = getBuffer2D( line, arg, false, pgvec );
	boost::geometry::model::linestring<BoostPoint2D> ls;
	for ( int i=0; i < line.size(); ++i ) {
		boost::geometry::append( ls, BoostPoint2D( jagatof(line.point[i].x), jagatof(line.point[i].y) ) );
		prt(("s1129 append i=%d x=%s y=%s\n", i, line.point[i].x, line.point[i].y ));
	}

	bool rc = getBuffer2D<boost::geometry::model::linestring<BoostPoint2D> >( ls, arg, pgvec );
	if ( ! rc ) {
		prt(("s2428 getBuffer2D rc=%d error\n", rc ));
		return false;
	}

	rc = get2DStrFromMultiPolygon( pgvec, srid, value );
	return rc;

}

bool JagCGAL::getBufferMultiPoint2DStr( const JagLineString &line, int srid, const AbaxDataString &arg, AbaxDataString &value )
{
	boost::geometry::model::multi_point<BoostPoint2D> mpoint;
	for ( int i=0; i < line.size(); ++i ) {
		boost::geometry::append( mpoint, BoostPoint2D( jagatof(line.point[i].x), jagatof(line.point[i].y) ) );
		prt(("s1129 append i=%d x=%s y=%s\n", i, line.point[i].x, line.point[i].y ));
	}
	JagVector<JagPolygon> pgvec;
	bool rc = getBuffer2D<boost::geometry::model::multi_point<BoostPoint2D> >( mpoint, arg, pgvec );
	if ( ! rc ) {
		prt(("s2428 getBuffer2D rc=%d error\n", rc ));
		return false;
	}

	rc = get2DStrFromMultiPolygon( pgvec, srid, value );
	return rc;
}

bool JagCGAL::getBufferPolygon2DStr(  const JagPolygon &pgon, int srid, const AbaxDataString &arg, AbaxDataString &value )
{
	boost::geometry::model::polygon<BoostPoint2D,false> bgon;
	for ( int i=0; i < pgon.size(); ++i ) {
		const JagLineString3D &linestr = pgon.linestr[i];
		std::vector< BoostPoint2D > pointList; 
		BoostRingType  ring;
		for (  int j=0; j< linestr.size(); ++j ) {
			BoostPoint2D p2( linestr.point[j].x, linestr.point[j].y );
			pointList.push_back(p2);
		}

		boost::geometry::assign_points( ring, pointList );
		boost::geometry::append( bgon, ring );
	}

	JagVector<JagPolygon> pgvec;
	bool rc = getBuffer2D<boost::geometry::model::polygon<BoostPoint2D,false> >( bgon, arg, pgvec );
	if ( ! rc ) {
		prt(("s2428 getBuffer2D rc=%d error\n", rc ));
		return false;
	}

	rc = get2DStrFromMultiPolygon( pgvec, srid, value );
	return rc;
}

bool JagCGAL::getBufferMultiLineString2DStr(  const JagPolygon &pgon, int srid, const AbaxDataString &arg, AbaxDataString &value )
{
	boost::geometry::model::multi_linestring<BoostLineString2D> mlstr;
	for ( int i=0; i < pgon.size(); ++i ) {
		const JagLineString3D &linestr = pgon.linestr[i];
		//BoostRingType ring;
		std::vector< BoostPoint2D > pointList; 
		for (  int j=0; j< linestr.size(); ++j ) {
			BoostPoint2D p2( linestr.point[j].x, linestr.point[j].y );
			pointList.push_back(p2);
		}

		boost::geometry::append( mlstr, pointList );
	}

	JagVector<JagPolygon> pgvec;
	bool rc = getBuffer2D<boost::geometry::model::multi_linestring<BoostLineString2D> >( mlstr, arg, pgvec );
	if ( ! rc ) {
		prt(("s2428 getBuffer2D rc=%d error\n", rc ));
		return false;
	}

	rc = get2DStrFromMultiPolygon( pgvec, srid, value );
	return rc;
	//return true;
}

bool JagCGAL::getBufferMultiPolygon2DStr(  const JagVector<JagPolygon> &pgvec, int srid, const AbaxDataString &arg, AbaxDataString &value )
{
	boost::geometry::model::multi_polygon<BoostPolygon2D> mbgon;
	for ( int k=0; k < pgvec.size(); ++k ) {
		boost::geometry::model::polygon<BoostPoint2D,false> bgon;
		const JagPolygon &pgon = pgvec[k];
		for ( int i=0; i < pgon.size(); ++i ) {
    		const JagLineString3D &linestr = pgon.linestr[i];
    		BoostRingType ring;
    		std::vector< BoostPoint2D > pointList; 
    		for (  int j=0; j< linestr.size(); ++j ) {
    			BoostPoint2D p2( linestr.point[j].x, linestr.point[j].y );
    			pointList.push_back(p2);
    		}
    		boost::geometry::assign_points( ring, pointList );
			boost::geometry::append( bgon, ring );
		}
    	mbgon.push_back( bgon );
	}

	JagVector<JagPolygon> pgvec2;
	bool rc = getBuffer2D<boost::geometry::model::multi_polygon<BoostPolygon2D> >( mbgon, arg, pgvec2 );
	if ( ! rc ) {
		prt(("s2428 getBuffer2D rc=%d error\n", rc ));
		return false;
	}

	rc = get2DStrFromMultiPolygon( pgvec2, srid, value );

	//double ds = boost::geometry::perimeter( mbgon );

	return rc;
}


// common method
bool JagCGAL::get2DStrFromMultiPolygon( const JagVector<JagPolygon> &pgvec, int srid, AbaxDataString &value )
{
	if ( pgvec.size() < 1 ) {
		return false;
	}

	double xmin, ymin, xmax, ymax;
	AbaxDataString s1, s2, s3, s4;
	bool rc = JagGeo::getBBox2D( pgvec, xmin, ymin, xmax, ymax );
	if ( ! rc ) return false;
	AbaxDataString newbbox;
	s1 = doubleToStr( xmin ).trimEndZeros();
	s2 = doubleToStr( ymin ).trimEndZeros();
	s3 = doubleToStr( xmax ).trimEndZeros();
	s4 = doubleToStr( ymax ).trimEndZeros();
	newbbox = s1 + ":" + s2 + ":" + s3 + ":" + s4;
	AbaxDataString nhdr = AbaxDataString(JAG_OJAG) + "=" + intToStr(srid) + "=dummy.dummy.dummy=MG=d";
	value = nhdr + " " + newbbox;
	AbaxDataString sx, sy;

	/***
	const JagPolygon &pgon = pgvec[0];
	const JagLineString3D &linestr = pgon.linestr[0];
	for ( int i=0; i < linestr.size(); ++i ) {
		value += AbaxDataString(" ") + doubleToStr(linestr.point[i].x) + ":" 
			     +  doubleToStr(linestr.point[i].y);
	}
	***/
	for ( int i = 0; i < pgvec.size(); ++i ) {
		const JagPolygon &pgon = pgvec[i];
		if ( i > 0 ) { value += "!"; }
		for ( int j=0; j < pgon.size(); ++j ) {
			const JagLineString3D &linestr = pgon.linestr[j];
			if ( j > 0 ) { value += "|"; }
			for ( int k=0; k < linestr.size(); ++k ) {
				value += AbaxDataString(" ") + doubleToStr(linestr.point[k].x) + ":" 
			     		 +  doubleToStr(linestr.point[k].y);
			}
		}
	}

	return true;
}


// arg: "distance=symmetric:20, side=side, join=round:5, end=round:90, point=circle:30
// arg: "distance=asymmetric:20:30, side=side, join=miter:5, end=flat, point=square
bool JagCGAL::createStrategies( JagStrategy *sptr[], const AbaxDataString &arg )
{
	JagStrSplit sp( arg, ',', true );
	AbaxDataString eq;
	int cnt = 0;
	bool hasDistance = false;
	bool hasSide = false;
	bool hasJoin = false;
	bool hasEnd = false;
	bool hasPoint = false;
	for ( int i=0; i < 5; ++i ) { sptr[i] = NULL; }

	for ( int i=0; i < sp.length(); ++i ) {
		eq = sp[i];
		eq.remove(' ');
		JagStrSplit speq(eq, '=');
		prt(("s8330 eq=[%s] speq.size=%d\n", eq.c_str(), speq.length() ));
		if ( speq.length() < 2 ) { continue; }

		if ( speq[0].caseEqual("distance") ) {
			JagStrSplit s3(speq[1], ':');
			if ( s3.length() < 2 ) continue;
			if ( s3[0] == "symmetric" ) {
				sptr[0] = new JagStrategy("distance_symmetric", jagatof( s3[1] ), 0 );
				++cnt;
			} else {
				if ( s3.length() < 3 ) continue;
				sptr[0] = new JagStrategy("distance_asymmetric", jagatof( s3[1] ), jagatof( s3[2] ) );
				++cnt;
			}
			hasDistance = true;
		} else if ( speq[0].caseEqual("side") ) {
			sptr[1] = new JagStrategy("side_straight", 0, 0 );
			++cnt;
			hasSide = true;
		} else if ( speq[0].caseEqual("join") ) {
			JagStrSplit s3(speq[1], ':');
			if ( s3.length() < 2 ) continue;
			if ( s3[0] == "round" ) {
				sptr[2] = new JagStrategy("join_round", jagatof( s3[1] ), 0 );
				++cnt;
			} else {
				sptr[2] = new JagStrategy("join_miter", jagatof( s3[1] ), 0 );
				++cnt;
			}
			hasJoin = true;
		} else if ( speq[0].caseEqual("end") ) {
			// end=round:32  end=flat
			if ( speq[1] == "flat" ) {
				sptr[3] = new JagStrategy("end_flat", 0, 0 );
				++cnt;
			} else {
				JagStrSplit s3(speq[1], ':');
				if ( s3.length() < 2 ) continue;
				sptr[3] = new JagStrategy("end_round", jagatof( s3[1] ), 0 );
				++cnt;
			}
			hasEnd = true;
		} else if ( speq[0].caseEqual("point") ) {
			// point=circle:32  point=square
			if ( speq[1] == "square" ) {
				sptr[4] = new JagStrategy("point_square", 0, 0 );
				++cnt;
			} else {
				JagStrSplit s3(speq[1], ':');
				if ( s3.length() < 2 ) continue;
				sptr[4] = new JagStrategy("point_circle", jagatof( s3[1] ), 0 );
				++cnt;
			}
			hasPoint = true;
		}
	}

	prt(("s1291 hasDistance=%d hasSide=%d hasJoin=%d hasEnd=%d hasPoint=%d\n", hasDistance, hasSide, hasJoin, hasEnd, hasPoint ));

	if ( ! hasDistance ) {
		sptr[0] = new JagStrategy("distance_symmetric", 10.0, 0 );
		++cnt;
	}

	if ( ! hasSide ) {
		sptr[1] = new JagStrategy("side_straight", 0, 0 );
		++cnt;
	}

	if ( ! hasJoin ) {
		sptr[2] = new JagStrategy("join_round", 10, 0 );
		++cnt;
	}

	if ( ! hasEnd ) {
		sptr[3] = new JagStrategy("end_round", 10, 0 );
		++cnt;
	}

	if ( ! hasPoint ) {
		sptr[4] = new JagStrategy("point_circle", 10, 0 );
		++cnt;
	}

	// deug
	for ( int i=0; i < 5; ++i ) { 
		prt(("s2201 i=%d name=%s  t=%d ptr=%0x\n", i, sptr[i]->name.c_str(), sptr[i]->t, sptr[i]->ptr ));
	}
	return true;
}

void JagCGAL::destroyStrategies( JagStrategy *sptr[] )
{
	for ( int i=0; i < 5; ++i ) { 
		if ( sptr[i] ) { delete sptr[i]; sptr[i] = NULL; }
	}
}

JagStrategy::JagStrategy( const AbaxDataString &nm, double a1, double a2 )
{
	name = nm;
	f1 = a1;
	f2 = a2;
	if ( name == "distance_symmetric" ) {
		JagDistanceSymmetric *p = new JagDistanceSymmetric(f1);
		ptr = (void*)p;
		t = JAG_DIST_SYMMETRIC;
	} else if ( name == "distance_asymmetric" ) {
		JagDistanceASymmetric *p = new JagDistanceASymmetric(f1,f2);
		ptr = (void*)p;
		t = JAG_DIST_ASYMMETRIC;
	} else if ( name == "side_straight" ) {
		JagSideStraight *p = new JagSideStraight();
		ptr = (void*)p;
		t = JAG_SIDE_STRAIGHT;
	} else if ( name == "join_round" ) {
		JagJoinRound *p = new JagJoinRound(int(f1));
		ptr = (void*)p;
		t = JAG_JOIN_ROUND;;
	} else if ( name == "join_miter" ) {
		JagJoinMiter *p = new JagJoinMiter(f1);
		ptr = (void*)p;
		t = JAG_JOIN_MITER;;
	} else if ( name == "end_round" ) {
		JagEndRound *p = new JagEndRound(int(f1));
		ptr = (void*)p;
		t = JAG_END_ROUND;
	} else if ( name == "end_flat" ) {
		JagEndFlat *p = new JagEndFlat();
		ptr = (void*)p;
		t = JAG_END_FLAT;
	} else if ( name == "point_circle" ) {
		JagPointCircle *p = new JagPointCircle((int)f1);
		ptr = (void*)p;
		t = JAG_POINT_CIRCLE;
	} else if ( name == "point_square" ) {
		JagPointSquare *p = new JagPointSquare();
		ptr = (void*)p;
		t = JAG_POINT_SQUARE;
	} else {
		prt(("s1020 strategy ctor name=[%s]\n", name.c_str() ));
		ptr = NULL;
		t = 0;
	}

}

JagStrategy::~JagStrategy()
{
	if ( name == "distance_symmetric" ) {
		JagDistanceSymmetric *p;
		p = (JagDistanceSymmetric*)ptr;
		delete p;
	} else if ( name == "distance_asymmetric" ) {
		JagDistanceASymmetric *p;
		p = (JagDistanceASymmetric*)ptr;
		delete p;
	} else if ( name == "side_straight" ) {
		JagSideStraight *p;
		p = (JagSideStraight*)ptr;
		delete p;
	} else if ( name == "join_round" ) {
		JagJoinRound *p;
		p = (JagJoinRound*)ptr;
		ptr = (void*)p;
	} else if ( name == "join_miter" ) {
		JagJoinMiter *p;
		p = new JagJoinMiter(f1);
		delete p;
	} else if ( name == "end_round" ) {
		JagEndRound *p;
		p = (JagEndRound*)ptr;
		delete p;
	} else if ( name == "end_flat" ) {
		JagEndFlat *p;
		p = (JagEndFlat*)ptr;
		delete p;
	} else if ( name == "point_circle" ) {
		JagPointCircle *p;
		p = (JagPointCircle*)ptr;
		delete p;
	} else if ( name == "point_square" ) {
		JagPointSquare *p;
		p = (JagPointSquare*)ptr;
		delete p;
	} else {
	}

}


// IsSimple methods
bool JagCGAL::getIsSimpleLineString2DStr( const JagLineString &line )
{
	//JagVector<JagPolygon> pgvec;
	boost::geometry::model::linestring<BoostPoint2D> ls;
	for ( int i=0; i < line.size(); ++i ) {
		boost::geometry::append( ls, BoostPoint2D( jagatof(line.point[i].x), jagatof(line.point[i].y) ) );
		prt(("s1129 append i=%d x=%s y=%s\n", i, line.point[i].x, line.point[i].y ));
	}

	//bool rc = boost::geometry::is_simple(multi_linestring);
	bool rc = boost::geometry::is_simple( ls );
	return rc;
}

bool JagCGAL::getIsSimplePolygon2DStr(  const JagPolygon &pgon ) 
{
	boost::geometry::model::polygon<BoostPoint2D,false> bgon;
	for ( int i=0; i < pgon.size(); ++i ) {
		const JagLineString3D &linestr = pgon.linestr[i];
		std::vector< BoostPoint2D > pointList; 
		BoostRingType  ring;
		for (  int j=0; j< linestr.size(); ++j ) {
			BoostPoint2D p2( linestr.point[j].x, linestr.point[j].y );
			pointList.push_back(p2);
		}

		boost::geometry::assign_points( ring, pointList );
		boost::geometry::append( bgon, ring );
	}

	bool rc = boost::geometry::is_simple( bgon );
	return rc;
}

bool JagCGAL::getIsSimpleMultiLineString2DStr(  const JagPolygon &pgon )
{
	boost::geometry::model::multi_linestring<BoostLineString2D> mlstr;
	for ( int i=0; i < pgon.size(); ++i ) {
		const JagLineString3D &linestr = pgon.linestr[i];
		//BoostRingType ring;
		std::vector< BoostPoint2D > pointList; 
		for (  int j=0; j< linestr.size(); ++j ) {
			BoostPoint2D p2( linestr.point[j].x, linestr.point[j].y );
			pointList.push_back(p2);
		}

		boost::geometry::append( mlstr, pointList );
	}

	bool rc = boost::geometry::is_simple( mlstr );
	return rc;
	//return true;
}

bool JagCGAL::getIsSimpleMultiPolygon2DStr(  const JagVector<JagPolygon> &pgvec )
{
	boost::geometry::model::multi_polygon<BoostPolygon2D> mbgon;
	for ( int k=0; k < pgvec.size(); ++k ) {
		boost::geometry::model::polygon<BoostPoint2D,false> bgon;
		const JagPolygon &pgon = pgvec[k];
		for ( int i=0; i < pgon.size(); ++i ) {
    		const JagLineString3D &linestr = pgon.linestr[i];
    		BoostRingType ring;
    		std::vector< BoostPoint2D > pointList; 
    		for (  int j=0; j< linestr.size(); ++j ) {
    			BoostPoint2D p2( linestr.point[j].x, linestr.point[j].y );
    			pointList.push_back(p2);
    		}
    		boost::geometry::assign_points( ring, pointList );
			boost::geometry::append( bgon, ring );
		}
    	mbgon.push_back( bgon );
	}

	bool rc = boost::geometry::is_simple( mbgon );
	return rc;
}


// IsValid methods
bool JagCGAL::getIsValidLineString2DStr( const JagLineString &line )
{
	//JagVector<JagPolygon> pgvec;
	boost::geometry::model::linestring<BoostPoint2D> ls;
	for ( int i=0; i < line.size(); ++i ) {
		boost::geometry::append( ls, BoostPoint2D( jagatof(line.point[i].x), jagatof(line.point[i].y) ) );
		prt(("s1129 append i=%d x=%s y=%s\n", i, line.point[i].x, line.point[i].y ));
	}

	//bool rc = boost::geometry::is_Valid(multi_linestring);
	boost::geometry::validity_failure_type failure;
	bool rc = boost::geometry::is_valid( ls, failure );
	return rc;
}

bool JagCGAL::getIsValidPolygon2DStr(  const JagPolygon &pgon ) 
{
	boost::geometry::model::polygon<BoostPoint2D,false> bgon;
	for ( int i=0; i < pgon.size(); ++i ) {
		const JagLineString3D &linestr = pgon.linestr[i];
		std::vector< BoostPoint2D > pointList; 
		BoostRingType  ring;
		for (  int j=0; j< linestr.size(); ++j ) {
			BoostPoint2D p2( linestr.point[j].x, linestr.point[j].y );
			pointList.push_back(p2);
		}

		boost::geometry::assign_points( ring, pointList );
		boost::geometry::append( bgon, ring );
	}

	boost::geometry::validity_failure_type failure;
	bool rc = boost::geometry::is_valid( bgon, failure );
	return rc;
}

bool JagCGAL::getIsValidMultiLineString2DStr(  const JagPolygon &pgon )
{
	boost::geometry::model::multi_linestring<BoostLineString2D> mlstr;
	for ( int i=0; i < pgon.size(); ++i ) {
		const JagLineString3D &linestr = pgon.linestr[i];
		//BoostRingType ring;
		std::vector< BoostPoint2D > pointList; 
		for (  int j=0; j< linestr.size(); ++j ) {
			BoostPoint2D p2( linestr.point[j].x, linestr.point[j].y );
			pointList.push_back(p2);
		}

		boost::geometry::append( mlstr, pointList );
	}

	boost::geometry::validity_failure_type failure;
	bool rc = boost::geometry::is_valid( mlstr, failure );
	return rc;
	//return true;
}

bool JagCGAL::getIsValidMultiPolygon2DStr(  const JagVector<JagPolygon> &pgvec )
{
	boost::geometry::model::multi_polygon<BoostPolygon2D> mbgon;
	for ( int k=0; k < pgvec.size(); ++k ) {
		boost::geometry::model::polygon<BoostPoint2D,false> bgon;
		const JagPolygon &pgon = pgvec[k];
		for ( int i=0; i < pgon.size(); ++i ) {
    		const JagLineString3D &linestr = pgon.linestr[i];
    		BoostRingType ring;
    		std::vector< BoostPoint2D > pointList; 
    		for (  int j=0; j< linestr.size(); ++j ) {
    			BoostPoint2D p2( linestr.point[j].x, linestr.point[j].y );
    			pointList.push_back(p2);
    		}
    		boost::geometry::assign_points( ring, pointList );
			boost::geometry::append( bgon, ring );
		}
    	mbgon.push_back( bgon );
	}

	boost::geometry::validity_failure_type failure;
	bool rc = boost::geometry::is_valid( mbgon, failure );
	return rc;
}

bool JagCGAL::getIsValidMultiPoint2DStr( const JagLineString &line )
{
	boost::geometry::model::multi_point<BoostPoint2D> mpoint;
	for ( int i=0; i < line.size(); ++i ) {
		boost::geometry::append( mpoint, BoostPoint2D( jagatof(line.point[i].x), jagatof(line.point[i].y) ) );
		prt(("s1129 append i=%d x=%s y=%s\n", i, line.point[i].x, line.point[i].y ));
	}

	boost::geometry::validity_failure_type failure;
	bool rc = boost::geometry::is_valid( mpoint, failure );
	return rc;
}


// IsRing methods
bool JagCGAL::getIsRingLineString2DStr( const JagLineString &line )
{
	boost::geometry::model::linestring<BoostPoint2D> ls;
	for ( int i=0; i < line.size(); ++i ) {
		boost::geometry::append( ls, BoostPoint2D( jagatof(line.point[i].x), jagatof(line.point[i].y) ) );
		prt(("s1129 append i=%d x=%s y=%s\n", i, line.point[i].x, line.point[i].y ));
	}

	//bool rc = boost::geometry::is_Valid(multi_linestring);
	bool rc = boost::geometry::is_simple( ls );
	if ( ! rc ) return false;

	// closed
	rc = ( line.point[0].equal2D( line.point[line.size()-1] ) );
	if ( ! rc ) return false;
	return true;
}

void JagCGAL::getRingStr( const JagLineString &line, const AbaxDataString &inhdr, const AbaxDataString &bbox, bool is3D,
							   AbaxDataString &value )
{
	const JagVector<JagPoint> &point = line.point;
	AbaxDataString sx, sy, sz;
	AbaxDataString hdr = inhdr;
	if ( is3D ) {
		hdr.replace("=PL3=", "=LS3=");
	} else {
		hdr.replace("=PL=", "=LS=");
	}

	if ( bbox.size() < 1 ) {
    	double xmin, ymin, xmax, ymax;
    	AbaxDataString s1, s2, s3, s4;
    	AbaxDataString newbbox;
		if ( is3D ) {
			double zmin, zmax;
    		AbaxDataString s5, s6;
    		line.bbox3D( xmin, ymin, zmin, xmax, ymax, zmax );
    		s1 = doubleToStr( xmin ).trimEndZeros();
    		s2 = doubleToStr( ymin ).trimEndZeros();
    		s3 = doubleToStr( zmin ).trimEndZeros();
    		s4 = doubleToStr( xmax ).trimEndZeros();
    		s5 = doubleToStr( ymax ).trimEndZeros();
    		s6 = doubleToStr( zmax ).trimEndZeros();
    		newbbox = s1 + ":" + s2 + ":" + s3 + ":" + s4  + ":" + s5 + ":" + s6;
		} else {
    		line.bbox2D( xmin, ymin, xmax, ymax );
    		s1 = doubleToStr( xmin ).trimEndZeros();
    		s2 = doubleToStr( ymin ).trimEndZeros();
    		s3 = doubleToStr( xmax ).trimEndZeros();
    		s4 = doubleToStr( ymax ).trimEndZeros();
    		newbbox = s1 + ":" + s2 + ":" + s3 + ":" + s4;
		}
    	value = hdr + " " + newbbox;
	} else {
		value = hdr + " " + bbox;
	}

	for ( int i = 0; i <  point.size(); ++i ) {
		sx = point[i].x; sx.trimEndZeros();
		sy = point[i].y; sy.trimEndZeros();
		if ( is3D ) {
			sz = point[i].z; sz.trimEndZeros();
			value += AbaxDataString(" ") + sx + ":" + sy + ":" + sz;
		} else {
			value += AbaxDataString(" ") + sx + ":" + sy;
		}
	}
}

void JagCGAL::getOuterRingsStr( const JagVector<JagPolygon> &pgvec, const AbaxDataString &inhdr, const AbaxDataString &bbox, 
								bool is3D, AbaxDataString &value )
{
	AbaxDataString sx, sy, sz;
	AbaxDataString hdr = inhdr;
	if ( is3D ) {
		hdr.replace("=MG3=", "=ML3=");
	} else {
		hdr.replace("=MG=", "=ML=");
	}

	if ( bbox.size() < 1 ) {
    	double xmin, ymin, xmax, ymax;
    	AbaxDataString s1, s2, s3, s4;
    	AbaxDataString newbbox;
		if ( is3D ) {
			double zmin, zmax;
    		AbaxDataString s5, s6;
    		JagGeo::getBBox3D( pgvec, xmin, ymin, zmin, xmax, ymax, zmax );
    		s1 = doubleToStr( xmin ).trimEndZeros();
    		s2 = doubleToStr( ymin ).trimEndZeros();
    		s3 = doubleToStr( zmin ).trimEndZeros();
    		s4 = doubleToStr( xmax ).trimEndZeros();
    		s5 = doubleToStr( ymax ).trimEndZeros();
    		s6 = doubleToStr( zmax ).trimEndZeros();
    		newbbox = s1 + ":" + s2 + ":" + s3 + ":" + s4  + ":" + s5 + ":" + s6;
		} else {
			JagGeo::getBBox2D( pgvec, xmin, ymin, xmax, ymax );
    		s1 = doubleToStr( xmin ).trimEndZeros();
    		s2 = doubleToStr( ymin ).trimEndZeros();
    		s3 = doubleToStr( xmax ).trimEndZeros();
    		s4 = doubleToStr( ymax ).trimEndZeros();
    		newbbox = s1 + ":" + s2 + ":" + s3 + ":" + s4;
		}
    	value = hdr + " " + newbbox;
	} else {
		value = hdr + " " + bbox;
	}

	for ( int i = 0; i <  pgvec.size(); ++i ) {
		const JagLineString3D &line = pgvec[i].linestr[0];
		if ( i > 0 && line.size() > 0 ) {
			value += AbaxDataString(" |");
		}

		for ( int j = 0; j < line.size(); ++j ) {
			const JagVector<JagPoint3D> &point = line.point;
    		sx = doubleToStr(point[j].x); sx.trimEndZeros();
    		sy = doubleToStr(point[j].y); sy.trimEndZeros();
    		if ( is3D ) {
    			sz = doubleToStr(point[j].z); sz.trimEndZeros();
    			value += AbaxDataString(" ") + sx + ":" + sy + ":" + sz;
    		} else {
    			value += AbaxDataString(" ") + sx + ":" + sy;
    		}
		}
	}

}


void JagCGAL::getInnerRingsStr( const JagVector<JagPolygon> &pgvec, const AbaxDataString &inhdr, const AbaxDataString &bbox, 
								bool is3D, AbaxDataString &value )
{
	prt(("s3381 getInnerRingsStr pgvec.size=%d\n", pgvec.size() ));
	AbaxDataString sx, sy, sz;
	AbaxDataString hdr = inhdr;
	if ( is3D ) {
		hdr.replace("=MG3=", "=ML3=");
	} else {
		hdr.replace("=MG=", "=ML=");
	}

	if ( bbox.size() < 1 ) {
    	double xmin, ymin, xmax, ymax;
    	AbaxDataString s1, s2, s3, s4;
    	AbaxDataString newbbox;
		if ( is3D ) {
			double zmin, zmax;
    		AbaxDataString s5, s6;
    		JagGeo::getBBox3DInner( pgvec, xmin, ymin, zmin, xmax, ymax, zmax );
    		s1 = doubleToStr( xmin ).trimEndZeros();
    		s2 = doubleToStr( ymin ).trimEndZeros();
    		s3 = doubleToStr( zmin ).trimEndZeros();
    		s4 = doubleToStr( xmax ).trimEndZeros();
    		s5 = doubleToStr( ymax ).trimEndZeros();
    		s6 = doubleToStr( zmax ).trimEndZeros();
    		newbbox = s1 + ":" + s2 + ":" + s3 + ":" + s4  + ":" + s5 + ":" + s6;
		} else {
			JagGeo::getBBox2DInner( pgvec, xmin, ymin, xmax, ymax );
    		s1 = doubleToStr( xmin ).trimEndZeros();
    		s2 = doubleToStr( ymin ).trimEndZeros();
    		s3 = doubleToStr( xmax ).trimEndZeros();
    		s4 = doubleToStr( ymax ).trimEndZeros();
    		newbbox = s1 + ":" + s2 + ":" + s3 + ":" + s4;
		}
    	value = hdr + " " + newbbox;
	} else {
		value = hdr + " " + bbox;
	}

	int numrings = 0;
	for ( int i = 0; i <  pgvec.size(); ++i ) {
		numrings =  pgvec[i].linestr.size();
		prt(("s9712 numrings=%d\n", numrings ));
		for ( int k = 1; k < numrings; ++k ) {
    		const JagLineString3D &line = pgvec[i].linestr[k];
    		if ( i > 0 && line.size() > 0 ) {
    			value += AbaxDataString(" |");
    		}
    
    		for ( int j = 0; j < line.size(); ++j ) {
    			const JagVector<JagPoint3D> &point = line.point;
        		sx = doubleToStr(point[j].x); sx.trimEndZeros();
        		sy = doubleToStr(point[j].y); sy.trimEndZeros();
        		if ( is3D ) {
        			sz = doubleToStr(point[j].z); sz.trimEndZeros();
        			value += AbaxDataString(" ") + sx + ":" + sy + ":" + sz;
        		} else {
        			value += AbaxDataString(" ") + sx + ":" + sy;
        		}
    		}
		}
	}
}


void JagCGAL::getPolygonNStr( const JagVector<JagPolygon> &pgvec, const AbaxDataString &inhdr, const AbaxDataString &bbox, 
								bool is3D, int N, AbaxDataString &value )
{
	prt(("s3381 getInnerRingsStr pgvec.size=%d\n", pgvec.size() ));
	if ( N > pgvec.size() ) {
		value = "";
		return;
	}
	AbaxDataString sx, sy, sz;
	AbaxDataString hdr = inhdr;
	if ( is3D ) {
		hdr.replace("=MG3=", "=PL3=");
	} else {
		hdr.replace("=MG=", "=PL=");
	}

	const JagPolygon &pgon = pgvec[N-1];

	if ( bbox.size() < 1 ) {
    	double xmin, ymin, xmax, ymax;
    	AbaxDataString s1, s2, s3, s4;
    	AbaxDataString newbbox;
		if ( is3D ) {
			double zmin, zmax;
    		AbaxDataString s5, s6;
    		pgon.bbox3D( xmin, ymin, zmin, xmax, ymax, zmax );
    		s1 = doubleToStr( xmin ).trimEndZeros();
    		s2 = doubleToStr( ymin ).trimEndZeros();
    		s3 = doubleToStr( zmin ).trimEndZeros();
    		s4 = doubleToStr( xmax ).trimEndZeros();
    		s5 = doubleToStr( ymax ).trimEndZeros();
    		s6 = doubleToStr( zmax ).trimEndZeros();
    		newbbox = s1 + ":" + s2 + ":" + s3 + ":" + s4  + ":" + s5 + ":" + s6;
		} else {
			pgon.bbox2D( xmin, ymin, xmax, ymax );
    		s1 = doubleToStr( xmin ).trimEndZeros();
    		s2 = doubleToStr( ymin ).trimEndZeros();
    		s3 = doubleToStr( xmax ).trimEndZeros();
    		s4 = doubleToStr( ymax ).trimEndZeros();
    		newbbox = s1 + ":" + s2 + ":" + s3 + ":" + s4;
		}
    	value = hdr + " " + newbbox;
	} else {
		value = hdr + " " + bbox;
	}

	for ( int k = 0; k < pgon.size(); ++k ) {
    	const JagLineString3D &line = pgon.linestr[k];
    	if ( k > 0 && line.size() > 0 ) {
    		value += AbaxDataString(" |");
    	}
   
   		for ( int j = 0; j < line.size(); ++j ) {
   			const JagVector<JagPoint3D> &point = line.point;
       		sx = doubleToStr(point[j].x); sx.trimEndZeros();
       		sy = doubleToStr(point[j].y); sy.trimEndZeros();
       		if ( is3D ) {
       			sz = doubleToStr(point[j].z); sz.trimEndZeros();
       			value += AbaxDataString(" ") + sx + ":" + sy + ":" + sz;
       		} else {
       			value += AbaxDataString(" ") + sx + ":" + sy;
      		}
   		}
	}
}
