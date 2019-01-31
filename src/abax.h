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
///////////////////////////////////////////////////////////////////////////////////////
//  Data Jaguar Inc
//
//  THIS SOFTWARE IS PROVIDED BY EXERAY INC "AS IS" AND ANY EXPRESS OR IMPLIED 
//  WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF 
//  MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO 
//  EVENT SHALL EXERAY INC BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, 
//  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT 
//  OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS 
//  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, 
//  STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY 
//  OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//
///////////////////////////////////////////////////////////////////////////////////////
#ifndef _jag_header_h_
#define _jag_header_h_

#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <limits.h>
#include <stdint.h>
#include <string>
#include <AbaxCStr.h>

typedef long long abaxint;
typedef unsigned long long uabaxint;
typedef AbaxCStr Jstr;

/////////////// Enum Types //////////////////
enum AbaxType { ABAX_STATIC = 10, ABAX_DYNAMIC = 20 };
enum AbaxDestroyAction { ABAX_NOOP=0, ABAX_FREE = 10 };
enum { ABAXITERALL=0, ABAXITERSTART=10, ABAXITERCOUNT=20, ABAXITERSTARTCOUNT=30, ABAXITERSTARTEND=40 };
enum AbaxBegin { ABAXNO=0, ABAXYES=1 };
enum AbaxStepAction { ABAX_NOMOVE=0, ABAX_NEXT=1 };
enum AbaxGraphType { ABAX_UNDIRECTED=0, ABAX_DIRECTED=1 };
enum AbaxLinkType { ABAX_DOUBLELINK=0, ABAX_INLINK=1, ABAX_OUTLINK=2 };
enum AbaxQueueType { ABAX_MINQUEUE=1, ABAX_MAXQUEUE=2 };

class abaxstream
{
	public:
	abaxstream & operator<< ( const char *str );
	abaxstream & operator<< ( int n );
	abaxstream & operator<< ( unsigned int n );
	abaxstream & operator<< ( unsigned char n );
	abaxstream & operator<< ( unsigned short n );
	abaxstream & operator<< ( float f);
	abaxstream & operator<< ( double d);
	abaxstream & operator<< ( long long l );
	abaxstream & operator<< ( unsigned long long l );
	// abaxstream & operator<< ( const Jstr &str );

};
extern abaxstream abaxcout;
extern const char* abaxendl; 

// Basic numerical types
template <class DataType>
class AbaxNumeric 
{
    template <class DataType2 > friend abaxstream& operator<< ( abaxstream &os, const AbaxNumeric<DataType2> & d );
    public:
        static DataType randomValue( int size=0 ) { return rand(); }  // todo
        static DataType randomLongValue( int size=0 ) { abaxint i=rand(); return i*2731+ rand(); } 
        static bool isString( ) { return false; }
		static AbaxNumeric NULLVALUE; 
        inline AbaxNumeric( ) { _data = 0; };
        inline AbaxNumeric( DataType d ) { _data = d ; }
        inline DataType value() const { return _data; }
        inline const char *addr() const { return (const char*)&_data; }
        inline const int addrlen() const { return 0; }
        inline const int size() const { return 4; }
		const char *c_str() { return ""; }
        inline abaxint hashCode() const { 
			DataType positive = _data; if ( _data < 0 ) positive = 0 - _data;
			return ( (uabaxint)positive + ((uabaxint)positive >>1)  ) % LLONG_MAX;
		}
        inline abaxint toLong() const { return (uabaxint)_data; }
        inline void destroy( AbaxDestroyAction action ) { };
		// inline Jstr toString() const { char buf[16]; sprintf(buf, "%lld", _data ); return buf; }
		inline void valueDestroy( AbaxDestroyAction action ) {}
        inline AbaxNumeric&  operator= ( const AbaxNumeric &s2 ) {
            _data = s2._data;
            return *this;
        }
        inline int operator== ( const AbaxNumeric &s2 ) const {
            return (_data == s2._data );
        }
        inline int operator!= ( const AbaxNumeric &s2 ) const {
            return ( ! ( _data == s2._data ) );
        }

        inline AbaxNumeric& increment(  ) {
            ++ _data; return *this;
        }
        inline AbaxNumeric& decrement(  ) {
            -- _data; return *this;
        }

        inline int operator< ( const AbaxNumeric &s2 ) const {
            return (_data < s2._data );
        }
        inline int operator<= ( const AbaxNumeric &s2 ) const {
            return (_data <= s2._data );
        }
        inline int operator> ( const AbaxNumeric &s2 ) const {
            return (_data > s2._data );
        }
        inline int operator>= ( const AbaxNumeric &s2 ) const {
            return (_data >= s2._data );
        }

        inline AbaxNumeric& operator+= ( const AbaxNumeric &s2 ) {
            _data +=  s2._data;
			return *this;
        }

    private:
        DataType _data;
};

template <class DataType>
class AbaxNumeric2 
{
    template <class DataType2 > friend abaxstream& operator<< ( abaxstream &os, const AbaxNumeric2<DataType2> & d );
    public:
        static DataType randomValue( int size ) { return rand(); } 
        static bool isString( ) { return false; }
		static AbaxNumeric2 NULLVALUE; 
        inline AbaxNumeric2( ) { data1 = 0; data2=0; };
        inline AbaxNumeric2( DataType d1, DataType d2 ) { data1 = d1 ; data2=d2; }

        inline DataType value() const { return data1; }
        inline const char *addr() const { return (const char*)&data1; }
        inline const int addrlen() const { return 0; }
        inline abaxint hashCode() const { return 0; }
        inline abaxint toLong() const { return (abaxint)data1; }

        inline void destroy( AbaxDestroyAction action ) { };
		inline Jstr toString() const { char buf[32]; sprintf(buf, "%lld%lld", data1, data2 ); return buf; }
		inline void valueDestroy( AbaxDestroyAction action ) {}

        inline AbaxNumeric2&  operator= ( const AbaxNumeric2 &s2 ) {
            data1 = s2.data1;
            data2 = s2.data2;
            return *this;
        }
        DataType data1;
        DataType data2;
};

void MurmurHash3_x64_128 ( const void * key, const int len, const unsigned int seed, void * out );
class AbaxString 
{
    friend abaxstream & operator<< ( abaxstream &os, const AbaxString& );
    public:
        static Jstr randomValue( int size=4)
		{
            int i, j;
            static char cset[] = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"; // 62 total
            char *mem = (char*)malloc(size+1);
            for ( i = 0; i< size; i++) {
                j = rand() % 62;
                mem[i] = cset[j];
            }
            mem[i] = '\0';
            Jstr str = mem;
            if ( mem ) free( mem );
            mem = 0;
            return str;
		}

        static bool isString( ) { return true; }
		static AbaxString NULLVALUE; 

        AbaxString( ) { };
        AbaxString( const Jstr &str ) { _str = str; }
        AbaxString( const char *str ) { if ( str ) _str = str; }
        AbaxString( const char *str, unsigned int len ) {
			_str = Jstr( str, len );
		}

        inline const Jstr &value() const { return _str; }
		inline const Jstr &toString()  const { return _str; }
		inline const char *addr() const { return _str.c_str(); }
		inline abaxint addrlen() const { return _str.size(); }
		inline const char *c_str() const { return _str.c_str(); }
		inline const char *s() const { return _str.c_str(); }
		inline abaxint size() const { return _str.size(); }
        ~AbaxString( ) { };

        inline abaxint hashCode() const 
		{
                unsigned int hash[4];                
                unsigned int seed = 42;             
                register void *str = (void *)this->_str.c_str();
                int len = _str.size();
                MurmurHash3_x64_128( str, len, seed, hash);
                uint64_t res2 = ((uint64_t*)hash)[0]; 
                abaxint res = res2 % LLONG_MAX;
            	return res;
		}

        inline void destroy( AbaxDestroyAction action ) { };
		inline void valueDestroy( AbaxDestroyAction action ) {}
        inline  AbaxString&  operator= ( const AbaxString &s2 ) {
            _str = Jstr( s2._str ); 
			return *this;
        }
        inline  int operator== ( const AbaxString &s2 )  const {
            return (_str == s2._str );
        }
        inline  int operator!= ( const AbaxString &s2 )  const {
            return ( ! (_str == s2._str ) );
        }
        inline  int operator< ( const AbaxString &s2 ) const {
            return (_str < s2._str );
        }
        inline  int operator<= ( const AbaxString &s2 ) const {
            return (_str <= s2._str );
        }
        inline  int operator> ( const AbaxString &s2 ) const {
            return (_str > s2._str );
        }
        inline  int operator>= ( const AbaxString &s2 ) const {
            return (_str >= s2._str );
        }

		inline AbaxString& operator+= (const AbaxString &s ) {
			_str += s._str;
			return *this;
		}

		inline AbaxString operator+ (const AbaxString &s ) const {
			AbaxString res = *this;
			res += s;
			return res;
		}

        inline abaxint toLong() const { return atol(_str.c_str()); }

    private:
        Jstr _str;
};

class JagFixString 
{
    public:
		static JagFixString NULLVALUE; 
        JagFixString();
        JagFixString( const char *str, unsigned int len ); 
        JagFixString( const char *str ); 
        // JagFixString( unsigned int len );
        JagFixString( const JagFixString &str ); 
		JagFixString( const Jstr &str );
        JagFixString& operator=( const char *str );
        JagFixString& operator=( const JagFixString &str );
		JagFixString& operator=( const Jstr &str ); 
        int operator== ( const JagFixString &s2 )  const;
        int operator< ( const JagFixString &s2 ) const;
        int operator<= ( const JagFixString &s2 ) const;
        int operator> ( const JagFixString &s2 ) const;
        int operator>= ( const JagFixString &s2 ) const;
		JagFixString& operator+= (const JagFixString &s );
		JagFixString operator+ (const JagFixString &s ) const;
		void point(const char *str, unsigned int len );
		void point(const JagFixString &str );
        JagFixString( const char *str, unsigned int len, bool ref );
		// caller make sure length(data) <= _length
		void strcpy( const char *data );
		~JagFixString();
		inline const char *addr() const { return _buf; }
		inline const char *c_str() const { return _buf; }
		inline const char *s() const { return _buf; }
		inline abaxint addrlen() const { return _length; }
		inline abaxint size() const { return _length; }
		inline abaxint length() const { return _length; }
        inline void destroy( AbaxDestroyAction action ) { };

        abaxint hashCode() const ;
		void ltrim();
		void rtrim();
		void trim();
		void substr( abaxint start, abaxint length = -1 );
		JagFixString concat( const JagFixString& secondStr );
		void  dump() const;
		void replace( char oldc, char newc );
		Jstr firstToken(char sep) const;

	private:
		char   *_buf;
		abaxint    _length;
		bool   _readOnly;

};

// A memory buffer type
class AbaxBuffer 
{
    friend abaxstream & operator<< ( abaxstream &os, const AbaxBuffer& );
    public:
        AbaxBuffer( ) {  _ptr = NULL; };
        static bool isString( ) { return false; }
		static AbaxBuffer NULLVALUE; 

         ~AbaxBuffer( ) {  };
        AbaxBuffer( void *ptr ) { _ptr = ptr; }
        inline void *value() const { return _ptr; }
        inline const char *addr() const { return (const char *)_ptr; }
		inline const char *c_str() const { return (const char *)_ptr; }
        inline const int addrlen() const { return 0; }
		inline const Jstr toString()  const { return ""; }

        abaxint hashCode() const { return 1; } 
        void destroy( AbaxDestroyAction action ) { 
            if ( _ptr ) {
                if ( action == ABAX_FREE ) { if ( _ptr ) free( _ptr ); _ptr = NULL; } 
                _ptr = NULL; 
            }
        }
		inline void valueDestroy( AbaxDestroyAction action ) { destroy(action); }
        static void * randomValue( int s ) {
            return (void*)0x83393;
        }

        inline AbaxBuffer&  operator= ( const AbaxBuffer &p2 ) {
            _ptr = p2._ptr; return *this;
        }
        inline AbaxBuffer&  operator= ( void *ptr ) {
            _ptr = ptr; return *this;
        }

        inline int operator== ( const AbaxBuffer &p2 )  const {
            return (_ptr == p2._ptr );
        }
        inline int operator!= ( const AbaxBuffer &p2 )  const {
            return ( ! (_ptr == p2._ptr ) );
        }
        inline int operator< ( const AbaxBuffer &p2 ) const {
            return (_ptr < p2._ptr );
        }
        inline int operator<= ( const AbaxBuffer &p2 ) const {
            return (_ptr <= p2._ptr );
        }
        inline  int operator> ( const AbaxBuffer &p2 ) const {
            return (_ptr > p2._ptr );
        }
        inline  int operator>= ( const AbaxBuffer &p2 ) const {
            return (_ptr >= p2._ptr );
        }

		inline AbaxBuffer& operator += ( const AbaxBuffer &p2 ) {
			return *this;
		}

        inline int toInt() const { return 0; }

    private:
        void *_ptr;

};

// typedef of data types
typedef AbaxNumeric<abaxint> AbaxLong;
typedef AbaxNumeric<int> AbaxInt;
typedef AbaxNumeric<short> AbaxShort;
typedef AbaxNumeric<float> AbaxFloat;
typedef AbaxNumeric<double> AbaxDouble;
typedef AbaxNumeric<char> AbaxChar;
typedef AbaxNumeric2<abaxint> AbaxLong2;


// Key-Value pair type
#pragma pack(4)
template <class KType,class VType>
class AbaxPair
{
    template <class K2,class V2> friend abaxstream& operator<< ( abaxstream &os, const AbaxPair<K2, V2> & pair );
    public:

        KType key;
        VType value;

		static  AbaxPair  NULLVALUE;
        inline AbaxPair() {}
        inline ~AbaxPair() {}
        inline AbaxPair( const KType &k) : key(k) {}
        inline AbaxPair( const KType &k, const VType &v) : key(k), value(v) {}
		inline AbaxPair( const AbaxPair& other ) 
		{
			key = other.key;
			value = other.value;
		}
		inline AbaxPair& operator= ( const AbaxPair& other )
		{
			if ( this == &other ) return *this;
			key = other.key;
			value = other.value;
			return *this;
		}

        inline int operator< ( const AbaxPair &d2 ) const {
           	return ( key < d2.key );
		}
        inline int operator<= ( const AbaxPair &d2 ) const {
           	return ( key <= d2.key );
		}
		
        inline int operator> ( const AbaxPair &d2 ) const {
           	return ( key > d2.key );
		}
        inline int operator>= ( const AbaxPair &d2 ) const {
           	return ( key >= d2.key );
		}

        inline int operator== ( const AbaxPair &d2 ) const
        {
           	return ( key == d2.key );
        }
        inline int operator!= ( const AbaxPair &d2 ) const
        {
            return ( ! ( key == d2.key ) );
        }

        inline void setValue( const VType &newvalue ) {
            value = newvalue;
        }

		inline void valueDestroy( AbaxDestroyAction action ) {
			value.destroy( action );
		}

		inline void destroy( AbaxDestroyAction action ) {
			value.destroy( action );
		}

        inline abaxint hashCode() const { return key.hashCode(); }

};
#pragma pack()


// Light-weight class containing only references to key-vaue
template <class K, class V>
class AbaxKeyValue
{ 
    public: 

    AbaxKeyValue(const K &k, V &v): key(k), value(v) {};
    AbaxKeyValue(const K &k ): key(k) {};
    AbaxKeyValue(const AbaxKeyValue<K,V> &ref) : key(ref.key), value(ref.value) {};

    const K &key; 
    V &value; 
};

#endif
