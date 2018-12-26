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

bool JagCGAL::getBuffer2D( const JagLineString &line, const AbaxDataString &arg, JagVector<JagPolygon> &mpgon )
{
	bool rc;
	JagStrategy *sptr[5];
	rc = createStrategies( sptr, arg );
	if ( ! rc ) {
		prt(("s2981 createStrategies error\n" ));
		return false;
	}

	boost::geometry::model::multi_polygon<CGALPolygon2D> result;
	boost::geometry::model::linestring<CGALPoint2D> ls;
	//line.print();
	for ( int i=0; i < line.size(); ++i ) {
		boost::geometry::append( ls, CGALPoint2D( jagatof(line.point[i].x), jagatof(line.point[i].y) ) );
		prt(("s1129 append i=%d x=%s y=%s\n", i, line.point[i].x, line.point[i].y ));
	}

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
	boost::geometry::model::multi_polygon<CGALPolygon2D>::iterator iter; 
	for ( iter = result.begin(); iter != result.end(); ++iter ) {
		prt(("s8272 iter :\n" ));
		//std::cout << *iter << std::endl;
		const CGALRingType &ring = iter->outer();
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


bool JagCGAL::getBuffer2DStr( const JagLineString &line, int srid, const AbaxDataString &hdr, const AbaxDataString &arg, AbaxDataString &value )
{
	JagVector<JagPolygon> pgvec;
	bool rc = getBuffer2D( line, arg, pgvec );
	if ( ! rc ) {
		prt(("s2428 getBuffer2D rc=%d error\n", rc ));
		return false;
	}
	if ( pgvec.size() < 1 ) {
		prt(("s2028 mpgon.size < 1 error\n" ));
		return false;
	}


	double xmin, ymin, xmax, ymax;
	AbaxDataString s1, s2, s3, s4;

	rc = JagGeo::getBBox2D( pgvec, xmin, ymin, xmax, ymax );
	if ( ! rc ) return false;
	AbaxDataString newbbox;
	s1 = doubleToStr( xmin ).trimEndZeros();
	s2 = doubleToStr( ymin ).trimEndZeros();
	s3 = doubleToStr( xmax ).trimEndZeros();
	s4 = doubleToStr( ymax ).trimEndZeros();
	newbbox = s1 + ":" + s2 + ":" + s3 + ":" + s4;
	// value = hdr + " " + newbbox;
	AbaxDataString nhdr = AbaxDataString(JAG_OJAG) + "=" + intToStr(srid) + "=dummy.dummy.dummy=PL=d";
	value = nhdr + " " + newbbox;

	prt(("s7733 getBuffer2DStr mpgon.size=%d \n", pgvec.size() ));
	AbaxDataString sx, sy;
	//pgvec.print();
	const JagPolygon &pgon = pgvec[0];
	const JagLineString3D &linestr = pgon.linestr[0];
	for ( int i=0; i < linestr.size(); ++i ) {
		value += AbaxDataString(" ") + doubleToStr(linestr.point[i].x) + ":" 
			     +  doubleToStr(linestr.point[i].y);
	}

}

bool JagCGAL::getBuffer2DStr( const JagPolygon &pgon, int srid, const AbaxDataString &hdr, const AbaxDataString &arg, AbaxDataString &value )
{
}

bool JagCGAL::getBuffer2D( const JagPolygon &pgon,  const AbaxDataString &arg, JagVector<JagPolygon> &mpgon )
{
}


bool JagCGAL::getBuffer2DStr( const JagVector<JagPolygon> &pgvec, int srid, const AbaxDataString &hdr, const AbaxDataString &arg, AbaxDataString &value )
{
}

bool JagCGAL::getBuffer2D( const JagVector<JagPolygon> &pgvec,  const AbaxDataString &arg, JagVector<JagPolygon> &mpgon )
{
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
