#include <JagGlobalDef.h>
#include <JagGeom.h>

JagPoint2D::JagPoint2D()
{
	x = y = 0.0;
}

JagPoint2D::JagPoint2D( const char *sx, const char *sy)
{
	x = jagatof(sx); y = jagatof( sy );
}

JagPoint2D::JagPoint2D( double inx, double iny )
{
	x = inx; y = iny;
}

JagPoint3D::JagPoint3D()
{
	x = y = z = 0.0;
}

JagPoint3D::JagPoint3D( const char *sx, const char *sy, const char *sz)
{
	x = jagatof(sx); y = jagatof( sy ); z = jagatof( sz );
}

JagPoint3D::JagPoint3D( double inx, double iny, double inz )
{
	x = inx; y = iny; z = inz;
}

Jstr JagPoint3D::hashString() const
{
	char buf[32];
	Jstr s;
	sprintf(buf, "%.6f", x );
	s = buf;

	sprintf(buf, "%.6f", y );
	s += Jstr(":") + buf;

	sprintf(buf, "%.6f", z );
	s += Jstr(":") + buf;

	return s;
}


bool JagSortPoint2D::operator<( const JagSortPoint2D &o) const
{
	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( x1 < o.x1 ) return true;
		} else {
			if ( x1 < o.x2 ) return true;
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( x2 < o.x1 ) return true;
		} else {
			if ( x2 < o.x2 ) return true;
		}
	}

	return false;
}
bool JagSortPoint2D::operator<=( const JagSortPoint2D &o) const
{
	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( x1 < o.x1 ) return true;
		} else {
			if ( x1 < o.x2 ) return true;
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( x2 < o.x1 ) return true;
		} else {
			if ( x2 < o.x2 ) return true;
		}
	}

	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( JagGeo::jagEQ(x1, o.x1) ) return true;
		} else {
			if ( JagGeo::jagEQ(x1, o.x2) ) return true;
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( JagGeo::jagEQ(x2, o.x1) ) return true;
		} else {
			if ( JagGeo::jagEQ(x2, o.x2) ) return true;
		}
	}
	return false;
}

bool JagSortPoint2D::operator>( const JagSortPoint2D &o) const
{
	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( x1 > o.x1 ) return true;
		} else {
			if ( x1 > o.x2 ) return true;
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( x2 > o.x1 ) return true;
		} else {
			if ( x2 > o.x2 ) return true;
		}
	}
	return false;
}


bool JagSortPoint2D::operator>=( const JagSortPoint2D &o) const
{
	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( x1 > o.x1 ) return true;
		} else {
			if ( x1 > o.x2 ) return true;
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( x2 > o.x1 ) return true;
		} else {
			if ( x2 > o.x2 ) return true;
		}
	}

	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( JagGeo::jagEQ(x1, o.x1) ) return true;
		} else {
			if ( JagGeo::jagEQ(x1, o.x2) ) return true;
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( JagGeo::jagEQ(x2, o.x1) ) return true;
		} else {
			if ( JagGeo::jagEQ(x2, o.x2) ) return true;
		}
	}

	return false;
}


bool JagSortPoint3D::operator<( const JagSortPoint3D &o) const
{
	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( x1 < o.x1 ) return true;
			if ( JagGeo::jagEQ( x1, o.x1) ) {
				if ( y1 < o.y1 ) return true;
			}
		} else {
			if ( x1 < o.x2 ) return true;
			if ( JagGeo::jagEQ( x1, o.x2) ) {
				if ( y1 < o.y2 ) return true;
			}
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( x2 < o.x1 ) return true;
			if ( JagGeo::jagEQ( x2, o.x1) ) {
				if ( y2 < o.y1 ) return true;
			}
		} else {
			if ( x2 < o.x2 ) return true;
			if ( JagGeo::jagEQ( x2, o.x2) ) {
				if ( y2 < o.y2 ) return true;
			}
		}
	}

	return false;
}

bool JagSortPoint3D::operator<=( const JagSortPoint3D &o) const
{

	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( x1 < o.x1 ) return true;
			if ( JagGeo::jagEQ( x1, o.x1) ) {
				if ( y1 < o.y1 ) return true;
			}
		} else {
			if ( x1 < o.x2 ) return true;
			if ( JagGeo::jagEQ( x1, o.x2) ) {
				if ( y1 < o.y2 ) return true;
			}
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( x2 < o.x1 ) return true;
			if ( JagGeo::jagEQ( x2, o.x1) ) {
				if ( y2 < o.y1 ) return true;
			}
		} else {
			if ( x2 < o.x2 ) return true;
			if ( JagGeo::jagEQ( x2, o.x2) ) {
				if ( y2 < o.y2 ) return true;
			}
		}
	}


	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( JagGeo::jagEQ(x1, o.x1) && JagGeo::jagEQ(y1, o.y1) ) return true;
		} else {
			if ( JagGeo::jagEQ(x1, o.x2) && JagGeo::jagEQ(y1, o.y2) ) return true;
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( JagGeo::jagEQ(x2, o.x1) && JagGeo::jagEQ(y2, o.y1) ) return true;
		} else {
			if ( JagGeo::jagEQ(x2, o.x2) && JagGeo::jagEQ(y2, o.y2) ) return true;
		}
	}
	return false;
}

bool JagSortPoint3D::operator>( const JagSortPoint3D &o) const
{
	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( x1 > o.x1 ) return 1;
			if ( JagGeo::jagEQ( x1, o.x1) ) {
				if ( y1 > o.y1 ) return -1;
			}
		} else {
			if ( x1 > o.x2 ) return 1;
			if ( JagGeo::jagEQ( x1, o.x2) ) {
				if ( y1 > o.y2 ) return 1;
			}
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( x2 > o.x1 ) return 1;
			if ( JagGeo::jagEQ( x2, o.x1) ) {
				if ( y2 > o.y1 ) return 1;
			}
		} else {
			if ( x2 > o.x2 ) return 1;
			if ( JagGeo::jagEQ( x2, o.x2) ) {
				if ( y2 > o.y2 ) return 1;
			}
		}
	}

	return false;
}


bool JagSortPoint3D::operator>=( const JagSortPoint3D &o) const
{
	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( x1 > o.x1 ) return true;
			if ( JagGeo::jagEQ( x1, o.x1) ) {
				if ( y1 > o.y1 ) return true;
			}
		} else {
			if ( x1 > o.x2 ) return true;
			if ( JagGeo::jagEQ( x1, o.x2) ) {
				if ( y1 > o.y2 ) return true;
			}
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( x2 > o.x1 ) return true;
			if ( JagGeo::jagEQ( x2, o.x1) ) {
				if ( y2 > o.y1 ) return true;
			}
		} else {
			if ( x2 > o.x2 ) return true;
			if ( JagGeo::jagEQ( x2, o.x2) ) {
				if ( y2 > o.y2 ) return true;
			}
		}
	}

	if ( JAG_LEFT == end ) {
		if ( JAG_LEFT == o.end ) {
			if ( JagGeo::jagEQ(x1, o.x1) && JagGeo::jagEQ(y1, o.y1) ) return true;
		} else {
			if ( JagGeo::jagEQ(x1, o.x2) && JagGeo::jagEQ(y1, o.y2) ) return true;
		}
	} else {
		if ( JAG_LEFT == o.end ) {
			if ( JagGeo::jagEQ(x2, o.x1) && JagGeo::jagEQ(y2, o.y1) ) return true;
		} else {
			if ( JagGeo::jagEQ(x2, o.x2) && JagGeo::jagEQ(y2, o.y2) ) return true;
		}
	}

	return false;
}


JagPoint::JagPoint() { init(); }
JagPoint::JagPoint( const char *inx, const char *iny )
{
	init();
	strcpy( x, inx );
	strcpy( y, iny );
}

JagPoint::JagPoint( const char *inx, const char *iny, const char *inz )
{
	init();
	strcpy( x, inx );
	strcpy( y, iny );
	strcpy( z, inz );
}

JagPoint& JagPoint::operator=( const JagPoint& p2 ) 
{
	if ( this == &p2 ) { return *this; }
	copyData( p2 );
	return *this; 
}

bool JagPoint::equal2D(const JagPoint &p ) const
{
	return JagGeo::jagEQ( jagatof(x), jagatof(p.x) ) && JagGeo::jagEQ( jagatof(y), jagatof(p.y) );
}
bool JagPoint::equal3D(const JagPoint &p ) const
{
	return JagGeo::jagEQ( jagatof(x), jagatof(p.x) ) && JagGeo::jagEQ( jagatof(y), jagatof(p.y) ) && JagGeo::jagEQ( jagatof(z), jagatof(p.z) );
}


JagPoint::JagPoint( const JagPoint& p2 ) 
{
	copyData( p2 );
}
void JagPoint::init() 
{
	memset( x, 0, JAG_POINT_LEN); memset(y, 0, JAG_POINT_LEN ); memset( z, 0, JAG_POINT_LEN); 
	memset( a, 0, JAG_POINT_LEN); memset( b, 0, JAG_POINT_LEN); memset( c, 0, JAG_POINT_LEN);
	memset( nx, 0, JAG_POINT_LEN); memset( ny, 0, JAG_POINT_LEN); 
}

void JagPoint::copyData( const JagPoint& p2 )
{
	memcpy( x, p2.x, JAG_POINT_LEN); 
	memcpy( y, p2.y, JAG_POINT_LEN ); 
	memcpy( z, p2.z, JAG_POINT_LEN); 
	memcpy( a, p2.a, JAG_POINT_LEN );
	memcpy( b, p2.b, JAG_POINT_LEN );
	memcpy( c, p2.c, JAG_POINT_LEN );
	memcpy( nx, p2.nx, JAG_POINT_LEN );
	memcpy( ny, p2.ny, JAG_POINT_LEN );
}

void JagPoint::print()  const
{
	prt(("x=[%s] y=[%s] z=[%s] a=[%s] b=[%s] c=[%s] nx=[%s] ny=[%s]\n", x,y,z,a,b,c,nx,ny ));

}

JagLineString& JagLineString::operator=( const JagLineString3D& L2 )
{
	init();
	for ( int i=0; i < L2.size(); ++i ) {
		add( L2.point[i] );
	}
	return *this;
}

JagLineString& JagLineString::copyFrom( const JagLineString3D& L2, bool removeLast )
{
	init();
	int len =  L2.size();
	if ( removeLast ) --len;
	for ( int i=0; i < len; ++i ) {
		add( L2.point[i] );
	}
	return *this;
}

JagLineString& JagLineString::appendFrom( const JagLineString3D& L2, bool removeLast )
{
	//init();
	int len =  L2.size();
	if ( removeLast ) --len;
	for ( int i=0; i < len; ++i ) {
		add( L2.point[i] );
	}
	return *this;
}

double JagLineString::lineLength( bool removeLast, bool is3D, int srid )
{
	double sum = 0.0;
	int len =  size();
	if ( removeLast ) --len;
	for ( int i=0; i < len-1; ++i ) {
		if ( is3D ) {
			sum += JagGeo::distance( jagatof(point[i].x), jagatof(point[i].y), jagatof(point[i].z),
									 jagatof(point[i+1].x), jagatof(point[i+1].y), jagatof(point[i+1].z), srid );
		} else {
			sum += JagGeo::distance( jagatof(point[i].x), jagatof(point[i].y),
									 jagatof(point[i+1].x), jagatof(point[i+1].y), srid );
		}
	}
	return sum;
}

void JagLineString::toJAG( bool is3D, bool hasHdr, const Jstr &inbbox, int srid, Jstr &str ) const
{
	if ( point.size() < 1 ) { str=""; return; }
	if ( hasHdr ) {
		Jstr srids = intToStr( srid );
		Jstr bbox;
		Jstr mk = "OJAG=";
		if ( is3D ) {
			if ( inbbox.size() < 1 )  { bbox = "0:0:0:0:0:0"; mk="CJAG="; } else { bbox = inbbox; }
			str = mk + srids + "=0=LS3=d " + bbox;
		} else {
			if ( inbbox.size() < 1 )  { bbox = "0:0:0:0"; mk="CJAG="; } else { bbox = inbbox; }
			str = mk + srids + "=0=LS=d " + bbox;
		}
	} 

	for ( int i=0; i < point.size(); ++i ) {
		str += Jstr(" ") + Jstr(point[i].x) + ":" +  Jstr(point[i].y);
		if ( is3D ) { str += Jstr(":") + Jstr(point[i].z); }
	}
}

void JagLineString3D::toJAG( bool is3D, bool hasHdr, const Jstr &inbbox, int srid, Jstr &str ) const
{
	if ( point.size() < 1 ) { str=""; return; }
	if ( hasHdr ) {
		Jstr srids = intToStr( srid );
		Jstr bbox;
		Jstr mk = "OJAG=";
		if ( is3D ) {
			if ( inbbox.size() < 1 )  { bbox = "0:0:0:0:0:0"; mk="CJAG="; } else { bbox = inbbox; }
			str = mk + srids + "=0=LS3=d " + bbox;
		} else {
			if ( inbbox.size() < 1 )  { bbox = "0:0:0:0"; mk="CJAG="; } else { bbox = inbbox; }
			str = mk + srids + "=0=LS=d " + bbox;
		}
	} 

	for ( int i=0; i < point.size(); ++i ) {
		str += Jstr(" ") + d2s(point[i].x) + ":" +  d2s(point[i].y);
		if ( is3D ) { str += Jstr(":") + d2s(point[i].z); }
	}
}

double JagLineString3D::lineLength( bool removeLast, bool is3D, int srid )
{
	double sum = 0.0;
	int len =  size();
	if ( removeLast ) --len;
	for ( int i=0; i < len-1; ++i ) {
		if ( is3D ) {
			sum += JagGeo::distance( point[i].x, point[i].y, point[i].z,
									 point[i+1].x, point[i+1].y, point[i+1].z, srid );
		} else {
			sum += JagGeo::distance( point[i].x, point[i].y,
									 point[i+1].x, point[i+1].y, srid);
		}
	}
	return sum;
}

// ratio: [0.0, 1.0]  returns point
bool JagLineString3D::interpolatePoint( short dim, int srid, double ratio, JagPoint3D &point )
{
	double length = lineLength(false, false, srid );
	double len = length * ratio;
	double segDist, segFraction;
	JagPoint3D p1, p2;
	bool rc = getBetweenPointsFromLen( dim, len, srid, p1, p2, segDist, segFraction );
	//prt(("s2831 getBetweenPointsFromLen rc=%d\n", rc ));
	if ( ! rc ) return false;

	if ( JAG_2D == dim && JAG_GEO_WGS84 == srid ) {
		JagGeo::interpolatePoint2D( segDist, segFraction, p1, p2, point );
	} else {
		point.x = p1.x + segFraction*(p2.x - p1.x);
		point.y = p1.y + segFraction*(p2.y - p1.y);
		if ( dim == JAG_3D ) { point.z = p1.z + segFraction*(p2.z - p1.z); }
	}

	return true;
}

bool JagLineString3D::getBetweenPointsFromLen( short dim, double len, int srid, JagPoint3D &p1, JagPoint3D &p2, 
											double &segDist, double &segFraction )
{
	//prt(("s6110 getBetweenPointsFromLen  dim=%d len=%f \n", dim, len ));
	double prevsegsum = 0.0;  // total len of all previous segments
	double segsum = 0.0;      // total len of all segments including current segment
	double dist = 0.0;
	int plen =  point.size();
	for ( int i=0; i < plen-1; ++i ) {
		if ( JAG_3D == dim  ) {
			dist = JagGeo::distance( point[i].x, point[i].y, point[i].z,
									 point[i+1].x, point[i+1].y, point[i+1].z, srid );
		} else {
			dist = JagGeo::distance( point[i].x, point[i].y,
									 point[i+1].x, point[i+1].y, srid );
		}

		segsum += dist;
		//prt(("s1028 dist=%.3f prevsegsum=%.3f segsum=%.3f\n", dist, prevsegsum, segsum ));

		if ( segsum > len || JagGeo::jagEQ( segsum, len) ) {
			p1.x = point[i].x; p1.y = point[i].y; 
			p2.x = point[i+1].x; p2.y = point[i+1].y; 
			if ( JAG_3D == dim  ) { p1.z = point[i].z; p2.z = point[i+1].z; }
			segDist = dist;
			segFraction = (len-prevsegsum)/dist;
			//prt(("s9483 retrn true\n" ));
			return true;
		}

		prevsegsum = segsum;
	}
	//prt(("s9283 retrn false\n" ));
	return false;
}

// ratio: [0.0, 1.0]  returns point
bool JagLineString3D::substring( short dim, int srid, double startFrac, double endFrac, Jstr &retLstr )
{
	// qwer todo
	double length = lineLength(false, false, srid );
	double len1 = length * startFrac;
	double len2 = length * endFrac;

	double prevsegsum = 0.0;  // total len of all previous segments
	double segsum = 0.0;      // total len of all segments including current segment
	double dist = 0.0;
	int plen =  point.size();
	bool inside = false;
	JagPoint3D p11, p12;
	JagPoint3D p21, p22;
	double segDist1, segFraction1;
	double segDist2, segFraction2;
	JagVector<JagPoint3D> pvec;

	for ( int i=0; i < plen-1; ++i ) {
		if ( JAG_3D == dim  ) {
			dist = JagGeo::distance( point[i].x, point[i].y, point[i].z,
									 point[i+1].x, point[i+1].y, point[i+1].z, srid );
		} else {
			dist = JagGeo::distance( point[i].x, point[i].y,
									 point[i+1].x, point[i+1].y, srid );
		}

		segsum += dist;
		//prt(("s1028 dist=%.3f prevsegsum=%.3f segsum=%.3f\n", dist, prevsegsum, segsum ));

		if ( ! inside ) {
    		if ( segsum > len1 || JagGeo::jagEQ( segsum, len1) ) {
    			p11.x = point[i].x; p11.y = point[i].y; 
    			p12.x = point[i+1].x; p12.y = point[i+1].y; 
    			if ( JAG_3D == dim  ) { p11.z = point[i].z; p12.z = point[i+1].z; }
    			segDist1 = dist;
    			segFraction1 = (len1-prevsegsum)/dist;
    			inside = true;
    		}
		}


		if ( inside ) {
    		if ( segsum > len2 || JagGeo::jagEQ( segsum, len2) ) {
    			p21.x = point[i].x; p21.y = point[i].y; 
    			p22.x = point[i+1].x; p22.y = point[i+1].y; 
    			if ( JAG_3D == dim  ) { p21.z = point[i].z; p22.z = point[i+1].z; }
    			segDist2 = dist;
    			segFraction2 = (len2-prevsegsum)/dist;
    			break;
    		} else {
				if ( JAG_3D == dim ) {
					pvec.append( JagPoint3D(point[i+1].x, point[i+1].y, point[i+1].z ) );
				} else {
					pvec.append( JagPoint3D(point[i+1].x, point[i+1].y, 0.0) );
				}
			}
		}

		prevsegsum = segsum;
	}

	JagPoint3D startPoint;
	if ( JAG_2D == dim && JAG_GEO_WGS84 == srid ) {
		JagGeo::interpolatePoint2D( segDist1, segFraction1, p11, p12, startPoint );
	} else {
		startPoint.x = p11.x + segFraction1*(p12.x - p11.x);
		startPoint.y = p11.y + segFraction1*(p12.y - p11.y);
		if ( dim == JAG_3D ) { startPoint.z = p11.z + segFraction1*(p12.z - p11.z); }
	}

	JagPoint3D endPoint;
	if ( JAG_2D == dim && JAG_GEO_WGS84 == srid ) {
		JagGeo::interpolatePoint2D( segDist2, segFraction2, p21, p22, endPoint );
	} else {
		endPoint.x = p21.x + segFraction2*(p22.x - p21.x);
		endPoint.y = p21.y + segFraction2*(p22.y - p21.y);
		if ( dim == JAG_3D ) { endPoint.z = p21.z + segFraction2*(p22.z - p21.z); }
	}


	if ( JAG_2D == dim ) {
		retLstr = d2s(startPoint.x) + ":" + d2s(startPoint.y);
		for ( int i=0; i < pvec.size(); ++i ) {
			retLstr += Jstr(" ") +  d2s(pvec[i].x) + ":" + d2s(pvec[i].y);
		}
		retLstr += Jstr(" " ) +  d2s(endPoint.x) + ":" + d2s(endPoint.y);
	} else {
		retLstr = d2s(startPoint.x) + ":" + d2s(startPoint.y) + ":" + d2s(startPoint.z);
		for ( int i=0; i < pvec.size(); ++i ) {
			retLstr += Jstr(" ") +  d2s(pvec[i].x) + ":" + d2s(pvec[i].y) + ":" + d2s(pvec[i].z);
		}
		retLstr += Jstr(" " ) +  d2s(endPoint.x) + ":" + d2s(endPoint.y) + ":" + d2s(endPoint.z);
	}
	
	return true;
}

void JagLineString::bbox2D( double &xmin, double &ymin, double &xmax, double &ymax ) const
{
	xmin = ymin = LONG_MAX;
	xmax = ymax = LONG_MIN;
	double f;
	for ( int i=0; i < point.size(); ++i ) {
		f = jagatof(point[i].x);
		if ( f < xmin ) xmin = f;
		if ( f > xmax ) xmax = f;

		f = jagatof(point[i].y);
		if ( f < ymin ) ymin = f;
		if ( f > ymax ) ymax = f;
	}
}

void JagLineString::bbox3D( double &xmin, double &ymin, double &zmin, double &xmax, double &ymax, double &zmax ) const
{
	xmin = ymin = zmin = LONG_MAX;
	xmax = ymax = zmax = LONG_MIN;
	double f;
	for ( int i=0; i < point.size(); ++i ) {
		f = jagatof(point[i].x);
		if ( f < xmin ) xmin = f;
		if ( f > xmax ) xmax = f;

		f = jagatof(point[i].y);
		if ( f < ymin ) ymin = f;
		if ( f > ymax ) ymax = f;

		f = jagatof(point[i].z);
		if ( f < zmin ) zmin = f;
		if ( f > zmax ) zmax = f;
	}
}



void JagLineString::add( const JagPoint2D &p )
{
	JagPoint pp( d2s(p.x).c_str(),  d2s(p.y).c_str() );
	point.append(pp);
}

void JagLineString::add( const JagPoint3D &p )
{
	JagPoint pp( d2s(p.x).c_str(),  d2s(p.y).c_str(), d2s(p.z).c_str() );
	point.append(pp);
}

void JagLineString::add( double x, double y )
{
	JagPoint pp( d2s(x).c_str(),  d2s(y).c_str() );
	point.append(pp);
}

void JagLineString::add( double x, double y, double z )
{
	JagPoint pp( d2s(x).c_str(),  d2s(y).c_str(), d2s(z).c_str() );
	point.append(pp);
}

void JagLineString::center2D( double &cx, double &cy, bool dropLast ) const
{
	cx = cy = 0.0;

	int len = point.size();
	if ( dropLast) --len;
	if ( len < 1 ) return;
	for ( int i=0; i < len; ++i ) {
		cx += jagatof(point[i].x);
		cy += jagatof(point[i].y);
	}
	cx = cx/len;
	cy = cy/len;
}
void JagLineString::center3D( double &cx, double &cy, double &cz, bool dropLast ) const
{
	cx = cy = cz = 0.0;
	int len = point.size();
	if ( dropLast) --len;
	if ( len < 1 ) return;
	for ( int i=0; i < len; ++i ) {
		cx += jagatof(point[i].x);
		cy += jagatof(point[i].y);
		cz += jagatof(point[i].z);
	}
	cx = cx/len;
	cy = cy/len;
	cz = cz/len;
}


void JagLineString3D::add( const JagPoint2D &p )
{
	JagPoint3D pp( d2s(p.x).c_str(),  d2s(p.y).c_str(), "0.0" );
	point.append(pp);
}

void JagLineString3D::add( const JagPoint3D &p )
{
	JagPoint3D pp( d2s(p.x).c_str(),  d2s(p.y).c_str(), d2s(p.z).c_str() );
	point.append(pp);
}

void JagLineString3D::add( double x, double y, double z )
{
	JagPoint3D pp;
	pp.x = x; pp.y = y; pp.z = z;
	point.append(pp);
}

void JagLineString3D::center3D( double &cx, double &cy, double &cz, bool dropLast ) const
{
	cx = cy = cz = 0.0;
	int len = point.size();
	if ( dropLast ) --len;
	if ( len < 1 ) return;
	for ( int i=0; i < len; ++i ) {
		cx += point[i].x;
		cy += point[i].y;
		cz += point[i].z;
	}
	cx = cx/len;
	cy = cy/len;
	cz = cz/len; 
}

void JagLineString3D::center2D( double &cx, double &cy, bool dropLast ) const
{
	cx = cy = 0.0;
	int len = point.size();
    if ( dropLast ) --len;
   	if ( len < 1 ) return;
	for ( int i=0; i < len; ++i ) {
		cx += point[i].x;
		cy += point[i].y;
	}
	cx = cx/len;
	cy = cy/len;
}

void JagLineString3D::bbox2D( double &xmin, double &ymin, double &xmax, double &ymax ) const
{
	xmin = ymin = LONG_MAX;
	xmax = ymax = LONG_MIN;
	double f;
	for ( int i=0; i < point.size(); ++i ) {
		f = point[i].x;
		if ( f < xmin ) xmin = f;
		if ( f > xmax ) xmax = f;

		f = point[i].y;
		if ( f < ymin ) ymin = f;
		if ( f > ymax ) ymax = f;
	}
}

void JagLineString3D::bbox3D( double &xmin, double &ymin, double &zmin,double &xmax, double &ymax, double &zmax ) const
{
	xmin = ymin = zmin = LONG_MAX;
	xmax = ymax = zmax = LONG_MIN;
	double f;
	for ( int i=0; i < point.size(); ++i ) {
		f = point[i].x;
		if ( f < xmin ) xmin = f;
		if ( f > xmax ) xmax = f;

		f = point[i].y;
		if ( f < ymin ) ymin = f;
		if ( f > ymax ) ymax = f;

		f = point[i].z;
		if ( f < zmin ) zmin = f;
		if ( f > zmax ) zmax = f;
	}
}

void JagLineString3D::reverse()
{
	int len = point.size();
	JagPoint3D t;
	for ( int i=0; i < len/2; ++i ) {
		if ( i != len-i-1 ) {
			t = point[i];
			point[i] = point[len-i-1];
			point[len-i-1] = t;
		}
	}
}

void JagLineString::reverse()
{
	int len = point.size();
	JagPoint t;
	for ( int i=0; i < len/2; ++i ) {
		if ( i != len-i-1 ) {
			t = point[i];
			point[i] = point[len-i-1];
			point[len-i-1] = t;
		}
	}
}


JagSquare2D::JagSquare2D(double inx, double iny, double ina, double innx, int insrid )
{
	init(inx, iny, ina, innx, insrid );
}

JagSquare2D::JagSquare2D( const JagStrSplit &sp, int insrid )
{
    double px = jagatof( sp[JAG_SP_START+0] );
    double py = jagatof( sp[JAG_SP_START+1] );
    double a = jagatof( sp[JAG_SP_START+2] );
    double nx = jagatof( sp[JAG_SP_START+3] );
    init(px,py, a, nx, insrid );
}

void JagSquare2D::init(double inx, double iny, double ina, double innx, int insrid )
{
	if ( JagGeo::jagGE( fabs(innx), 1.0 ) ) innx = 1.0;
	double ux = JagGeo::meterToLon( insrid, ina, inx, iny );
	double uy = JagGeo::meterToLat( insrid, ina, inx, iny );
	double a = ux * ( 1.0-innx*innx ) + uy * innx * innx;
	double b = uy * ( 1.0-innx*innx ) + ux * innx * innx;

	x0 =inx; y0 = iny; nx = innx; srid=insrid;

	if ( nx < 0.00001 || JagGeo::jagGE(nx, 1.0) ) {
    	JagPoint2D p( -a, -b ); 
    	point[0] = p;
    
    	p.x = a; p.y = -b;
    	point[1] = p;
    
    	p.x = a; p.y = b;
    	point[2] = p;
    
    	p.x = -a; p.y = b;
    	point[3] = p;
	} else {
		JagRectangle2D::setPoint( x0, y0, a, b, nx, point );
	}
}

JagRectangle2D::JagRectangle2D(double inx, double iny, double ina, double inb, double innx, int insrid )
{
	init(inx, iny, ina, inb, innx, insrid );
}

void JagRectangle2D::init(double inx, double iny, double ina, double inb, double innx, int insrid )
{
	//prt(("s28039 JagRectangle2D::init inx=%f iny=%f ina=%f inb=%f innx=%f insrid=%d\n", inx, iny, ina, inb, innx, insrid ));
	if ( JagGeo::jagGE( fabs(innx), 1.0 ) ) innx = 1.0;
	double ux = JagGeo::meterToLon( insrid, ina, inx, iny );
	double uy = JagGeo::meterToLat( insrid, inb, inx, iny );
	double a = ux * ( 1.0-innx*innx ) + uy * innx * innx;
	double b = uy * ( 1.0-innx*innx ) + ux * innx * innx;

	//prt(("s2049 JagRectangle2D::init ux=%f uy=%f a=%f b=%f\n", ux, uy, a, b ));
	x0 =inx; y0 = iny; nx = innx; srid=insrid;
	if ( nx < 0.00001 || JagGeo::jagGE(nx, 1.0) ) {
    	JagPoint2D p( -a, -b ); 
    	point[0] = p;
    
    	p.x = a; p.y = -b;
    	point[1] = p;
    
    	p.x = a; p.y = b;
    	point[2] = p;
    
    	p.x = -a; p.y = b;
    	point[3] = p;
	} else {
		JagRectangle2D::setPoint( x0, y0, a, b, nx, point );
	}
}

JagRectangle2D::JagRectangle2D( const JagStrSplit &sp, int insrid )
{
    double px = jagatof( sp[JAG_SP_START+0] );
    double py = jagatof( sp[JAG_SP_START+1] );
    double a = jagatof( sp[JAG_SP_START+2] );
    double b = jagatof( sp[JAG_SP_START+3] );
    double nx = jagatof( sp[JAG_SP_START+4] );
    init(px,py, a, b, nx, insrid );
}

void JagRectangle2D::setPoint( double x0, double y0, double a, double b, double nx, JagPoint2D point[] )
{
	// counter-clock-wise
   	JagPoint2D p( -a, -b ); 
   	p.transform( x0,y0,nx);
   	point[0] = p;
   
    p.x = a; p.y = -b;
    p.transform( x0,y0,nx);
    point[1] = p;
    
    p.x = a; p.y = b;
    p.transform( x0,y0,nx);
    point[2] = p;
    
    p.x = -a; p.y = b;
    p.transform( x0,y0,nx);
    point[3] = p;
}


JagCircle2D::JagCircle2D( double inx, double iny, double inr, int insrid )
{
	init( inx, iny, inr, insrid );
}

void JagCircle2D::init( double inx, double iny, double inr, int insrid )
{
	x0 =inx; y0 = iny; r = inr; srid = insrid;
}

JagCircle2D::JagCircle2D( const JagStrSplit &sp, int insrid )
{
	//prt(("s2277 JagCircle2D ctor sp:  insrid=%d\n", srid ));
	//sp.print();
    double px = jagatof( sp[JAG_SP_START+0] );
    double py = jagatof( sp[JAG_SP_START+1] );
    double r = jagatof( sp[JAG_SP_START+2] );
	//prt(("s9283 ctor of JagCircle2D px=%f py=%f r=%f\n", px, py, r ));
    init(px,py, r, insrid );
}

void JagEllipse2D::init( double inx, double iny, double ina, double inb, double innx, int insrid )
{
	x0 =inx; y0 = iny; a = ina; b = inb; nx = innx; srid=insrid;
}

JagEllipse2D::JagEllipse2D( double inx, double iny, double ina, double inb, double innx, int insrid )
{
    init( inx, iny, ina, inb, innx, insrid );
}

JagEllipse2D::JagEllipse2D( const JagStrSplit &sp, int insrid )
{
    double px = jagatof( sp[JAG_SP_START+0] );
    double py = jagatof( sp[JAG_SP_START+1] );
    double a = jagatof( sp[JAG_SP_START+2] );
    double b = jagatof( sp[JAG_SP_START+3] );
    double nx = jagatof( sp[JAG_SP_START+4] );
    init(px,py, a, b, nx, insrid );
}

JagTriangle2D::JagTriangle2D( const JagStrSplit &sp, int insrid )
{
    double x1 = jagatof( sp[JAG_SP_START+0] );
    double y1 = jagatof( sp[JAG_SP_START+1] );
    double x2 = jagatof( sp[JAG_SP_START+2] );
    double y2 = jagatof( sp[JAG_SP_START+3] );
    double x3 = jagatof( sp[JAG_SP_START+4] );
    double y3 = jagatof( sp[JAG_SP_START+5] );
	init( x1, y1, x2, y2, x3, y3, insrid );
}

JagTriangle2D::JagTriangle2D( double inx1, double iny1, double inx2, double iny2, double inx3, double iny3, int insrid )
{
	init( inx1, iny1, inx2, iny2, inx3, iny3, insrid );
}

void JagTriangle2D::init( double inx1, double iny1, double inx2, double iny2, double inx3, double iny3, int insrid )
{
	x1 = inx1; y1 = iny1;
	x2 = inx2; y2 = iny2;
	x3 = inx3; y3 = iny3;
	srid = insrid;
}


void JagPolygon::center2D(  double &cx, double &cy ) const
{
	cx = cy = 0.0;
	// JagVector<JagLineString3D> linestr;
	int len = linestr.size();
	//prt(("s10007 len=[%d]\n", len ));
	if ( len < 1 ) return;
	double x, y;
	for ( int i=0; i < len; ++i ) {
		linestr[i].center2D(x, y, true);
		cx += x;
		cy += y;
	}

	cx = cx / len;
	cy = cy / len;
}

void JagPolygon::center3D(  double &cx, double &cy, double &cz ) const
{
	cx = cy = cz = 0.0;
	int len = linestr.size();
	if ( len < 1 ) return;
	double x, y, z;
	for ( int i=0; i < len; ++i ) {
		linestr[i].center3D(x, y, z, true);

		cx += x;
		cy += y;
		cz += z;
	}

	cx = cx / len;
	cy = cy / len;
	cz = cz / len;
}

bool JagPolygon::bbox2D( double &xmin, double &ymin, double &xmax, double &ymax ) const
{
	int len = linestr.size();
	if ( len < 1 ) return false;
	double xmi, ymi, xma, yma;
	xmin = ymin =  LONG_MAX;
	xmax = ymax = LONG_MIN;
	int cnt = 0;
	for ( int i=0; i < len; ++i ) {
		if ( linestr[i].size() < 1 ) continue;
		linestr[i].bbox2D( xmi, ymi, xma, yma );
		if ( xmi < xmin ) xmin = xmi;
		if ( ymi < ymin ) ymin = ymi;
		if ( xma > xmax ) xmax = xma;
		if ( yma > ymax ) ymax = yma;
		++cnt;
	}

	return (cnt > 0 );
}

bool JagPolygon::bbox3D( double &xmin, double &ymin, double &zmin, double &xmax, double &ymax, double &zmax ) const
{
	int len = linestr.size();
	if ( len < 1 ) return false;
	double xmi, ymi, zmi, xma, yma, zma;
	xmin = ymin = zmin = LONG_MAX;
	xmax = ymax = zmax = LONG_MIN;
	for ( int i=0; i < len; ++i ) {
		linestr[i].bbox3D( xmi, ymi, zmi, xma, yma, zma );
		if ( xmi < xmin ) xmin = xmi;
		if ( ymi < ymin ) ymin = ymi;
		if ( zmi < zmin ) zmin = zmi;
		if ( xma > xmax ) xmax = xma;
		if ( yma > ymax ) ymax = yma;
		if ( zma > zmax ) zmax = zma;
	}

	return true;
}

double JagPolygon::lineLength( bool removeLast, bool is3D, int srid )
{
	int len = linestr.size();
	if ( len < 1 ) return 0.0;
	double sum = 0.0;
	for ( int i=0; i < len; ++i ) {
		sum += linestr[i].lineLength( removeLast, is3D, srid );
		//prt(("s0393 i=%d/%d sum=%f\n", i, len, sum ));
	}
	return sum;
}

void JagPolygon::toWKT( bool is3D, bool hasHdr, const Jstr &objname, Jstr &str ) const
{
	if ( linestr.size() < 1 ) { str=""; return; }
	if ( hasHdr ) {
		str = objname +  "(";
	} else {
		str = "(";
	}

	for ( int i=0; i < linestr.size(); ++i ) {
		if ( i==0 ) {
			str += "(";
		} else {
			str += ",(";
		}
		const JagLineString3D &lstr = linestr[i];
		for (  int j=0; j< lstr.size(); ++j ) {
			if ( j>0) { str += Jstr(","); }
			str += d2s(lstr.point[j].x) + " " +  d2s(lstr.point[j].y);
			if ( is3D ) { str += Jstr(" ") + d2s(lstr.point[j].z); }
		}
		str += ")";
	}
	
	str += ")";
}

void JagPolygon::toJAG( bool is3D, bool hasHdr,  const Jstr &inbbox, int srid, Jstr &str ) const
{
	if ( linestr.size() < 1 ) { str=""; return; }
	if ( hasHdr ) {
		Jstr srids = intToStr( srid );
		Jstr bbox;
		Jstr mk = "OJAG=";
		if ( is3D ) {
			if ( inbbox.size() < 1 )  { bbox = "0:0:0:0:0:0"; mk="CJAG="; } else { bbox = inbbox; }
			str = mk + srids + "=0=PL3=d " + bbox;
		} else {
			if ( inbbox.size() < 1 )  { bbox = "0:0:0:0"; mk="CJAG="; } else { bbox = inbbox; }
			str = mk + srids + "=0=PL=d " + bbox;
		}
	} 

	for ( int i=0; i < linestr.size(); ++i ) {
		if ( i>0 ) {
			str += " |";
		}
		const JagLineString3D &lstr = linestr[i];
		prt(("s1127 JagPolygon::toJAG i=%d lstr.size=%d\n", i, lstr.size() ));
		for (  int j=0; j< lstr.size(); ++j ) {
			str += Jstr(" ") + d2s(lstr.point[j].x) + ":" +  d2s(lstr.point[j].y);
			if ( is3D ) { str += Jstr(":") + d2s(lstr.point[j].z); }
		}
	}
	
}

