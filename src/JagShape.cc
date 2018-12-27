#include <JagGlobalDef.h>
#include <JagGeom.h>

JagSquare2D::JagSquare2D(double inx, double iny, double ina, double innx )
{
	x0 =inx; y0 = iny; a = ina; nx = innx;

	if ( nx < 0.00001 || JagGeo::jagGE(nx, 1.0) ) {
    	JagPoint2D p( -a, -a ); 
    	point[0] = p;
    
    	p.x = a; p.y = -a;
    	point[1] = p;
    
    	p.x = a; p.y = a;
    	point[2] = p;
    
    	p.x = -a; p.y = a;
    	point[3] = p;
	} else {
    	JagPoint2D p( -a, -a ); 
    	p.transform( x0,y0,nx);
    	point[0] = p;
    
    	p.x = a; p.y = -a;
    	p.transform( x0,y0,nx);
    	point[1] = p;
    
    	p.x = a; p.y = a;
    	p.transform( x0,y0,nx);
    	point[2] = p;
    
    	p.x = -a; p.y = a;
    	p.transform( x0,y0,nx);
    	point[3] = p;
	}
}

JagRectangle2D::JagRectangle2D(double inx, double iny, double ina, double inb, double innx )
{
	x0 =inx; y0 = iny; a = ina; b = inb; nx = innx;

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
}

JagCircle2D::JagCircle2D( double inx, double iny, double inr )
{
	x0 =inx; y0 = iny; r = inr;
}

JagEllipse2D::JagEllipse2D( double inx, double iny, double ina, double inb, double innx )
{
	x0 =inx; y0 = iny; a = ina; b = inb; nx = innx;
}

JagTriangle2D::JagTriangle2D( double inx1, double iny1, double inx2, double iny2, double inx3, double iny3 )
{
	x1 = inx1; y1 = iny1;
	x2 = inx2; y2 = iny2;
	x3 = inx3; y3 = iny3;
}

