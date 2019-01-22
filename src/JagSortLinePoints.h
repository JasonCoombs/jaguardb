/*
 * Copyright (C) 2018 DataJaguar, Inc.
 *
 * This file is part of JaguarDB.
 *
 * JaguarDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * JaguarDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with JaguarDB (LICENSE.txt). If not, see <http://www.gnu.org/licenses/>.
 */
#define  MAX_LEVELS  300
template <class POINT>
int JagGeo::sortLinePoints( POINT arr[], int elements )
{
    // int  piv, beg[MAX_LEVELS], end[MAX_LEVELS], i=0, L, R, swap;
    int  beg[MAX_LEVELS], end[MAX_LEVELS], i=0, L, R, swap;
	POINT piv;
    beg[0]=0; end[0]=elements;
    while (i>=0) {
        L=beg[i]; R=end[i]-1;
        if (L<R) {
            piv=arr[L];
            while (L<R) {
            	while ( arr[R] >= piv && L<R) { 
					// if arr[R] and piv intersect, return 1
					/***
					if ( arr[R].color != piv.color ) {
						if ( lineIntersectLine( arr[R].x1,arr[R].y1,arr[R].x2,arr[R].y2, 
											    piv.x1,piv.y1,piv.x2,piv.y2 ) ) {
							prt(("e6303 %f %f %f %f   %f %f %f %f  intersect\n",
								  arr[R].x1,arr[R].y1,arr[R].x2,arr[R].y2,
								  piv.x1,piv.y1,piv.x2,piv.y2  ));
							return 1;
						}
					}
					***/
					R--; 
				}
    			if (L<R) { arr[L++]=arr[R]; }
            	while (arr[L] <= piv && L<R) { 
					// if arr[L] and piv intersect, return 1
					// if ( lineIntersectLine(  x1, y1, x2, y2, x3, y3, x4, y4  ) ) return 1;
					/***
					if ( arr[L].color != piv.color ) {
						if ( lineIntersectLine( arr[L].x1,arr[L].y1,arr[L].x2,arr[L].y2, 
							 piv.x1,piv.y1,piv.x2,piv.y2 ) ) {
							prt(("e6304 %f %f %f %f   %f %f %f %f  intersect\n",
								  arr[L].x1,arr[L].y1,arr[L].x2,arr[L].y2,
								  piv.x1,piv.y1,piv.x2,piv.y2  ));
							 return 1;
					    }
					}
					***/
					L++; 
				}
    			if (L<R) { arr[R--]=arr[L];  }
    	    }
    
            arr[L]=piv; 
			beg[i+1]=L+1; end[i+1]=end[i]; end[i++]=L; 
    
            if (end[i]-beg[i]>end[i-1]-beg[i-1]) {
            	swap=beg[i]; beg[i]=beg[i-1]; beg[i-1]=swap;
            	swap=end[i]; end[i]=end[i-1]; end[i-1]=swap; 
    	  	} 
  	  	} else {
        	i--; 
  	  	}
   }

   return 0;
}

template <class POINT>
int JagGeo::JagSortedSetJoin( POINT arr1[], int len1,  POINT arr2[], int len2, JagVector<POINT> &vec )
{
	int i=0; int j=0;
	int cnt = 0;
	while ( true ) {
		if ( i >= len1 || j >= len2 ) break;
		if ( arr1[i] == arr2[j] ) {
			vec.append( arr1[i] );
			++cnt; ++i; ++j;
		} else if ( arr1[i] < arr2[j] ) {
			++i;
		} else {
			++j;
		}
	}
	return 0;
}


template <class POINT>
int JagGeo::JagSetJoin( POINT arr1[], int len1,  POINT arr2[], int len2, JagVector<POINT> &vec )
{
	sortLinePoints( arr1, len1 );
	sortLinePoints( arr2, len2 );
	JagSortedSetJoin( arr1, len1,  arr2, len2, vec );
}
