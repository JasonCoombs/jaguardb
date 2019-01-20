#include <JagGlobalDef.h>
#include <JagGeom.h>

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

