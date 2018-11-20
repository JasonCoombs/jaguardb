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
#ifndef _jag_list_h_
#define _jag_list_h_

#include "JagMutex.h"

template <class VType>
class JagListNode 
{ 
    public:
        VType value; 
        JagListNode *next; 
};

/***
  Simple list class
***/
template <class VType>
class JagList
{
    template <class T> friend class JagListIterator;

    public:

        JagList(); 
        JagList(const JagList<VType>& list);
        ~JagList(); 

        void  	add( const VType &value ); 
        bool  	exist( const VType &value ); 
        inline 	bool     isEmpty() const { return ( head_ == NULL ); } 
        bool    remove( const VType &value, AbaxDestroyAction action=ABAX_NOOP );
        bool    replace( const VType &oldValue, const VType &newValue );
        void    print() const; 
		void    copy( const JagList &list );

        void    destroy( AbaxDestroyAction action=ABAX_NOOP ); 
        JagList<VType>&  operator= ( const JagList<VType> &list ); 
        inline uabaxint  size() const { return length_; }
		void 	setConcurrent( bool flag = true );

    private:
        JagListNode<VType>     *head_;
        uabaxint           length_; 
		JagReadWriteLock  		*_lock;
};


/////////////////// abaxlist //////////////////////////
template <class VType>
JagList<VType>::JagList()
{
	head_ = NULL;
	length_ = 0;
	_lock = NULL;
}

template <class VType>
JagList<VType>::~JagList()
{
	// abaxcout << "c7731 abaxlist dtor called, freelist..." << abaxendl;
	// freeList();
	destroy();

	if ( _lock ) {
		delete _lock;
	}
	_lock = NULL;
}

template <class VType>
void JagList<VType>::destroy( AbaxDestroyAction action )
{
	if ( NULL != head_ )
	{
		JagListNode<VType> *q = head_;
		JagListNode<VType> *p = q;
		while(  p != NULL )
		{
			q = p->next;
			if ( action != ABAX_NOOP ) {
				p->value.destroy( action );
			}
			if ( p ) delete p;
			p = q;
		}

		head_ = NULL;
	}

	length_ = 0;

}

template <class VType>
JagList<VType>& JagList<VType>::operator= ( const JagList<VType> &list )
{
	JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );

	// self destroy first
	destroy();

	JagListNode<VType> *p = list.head_;
	while ( p ) {
		add( p->value );
		p = p->next;
	}

	return *this;
}

// always add a new one, even if it duplicates
// append at tail side
template <class VType>
void JagList<VType>::add( const VType &value )
{
	JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );

	JagListNode<VType> *pnew = new JagListNode<VType>();
	pnew->value = value;

	// pnew->next = NULL;
	pnew->next = head_;
	head_ = pnew;

	++length_;
}


// ctor
template <class VType>
JagList<VType>::JagList(const JagList<VType>& list)
{
	_lock = NULL;

	head_ = NULL;
	JagListNode<VType> *p = list.head_;
	while ( p ) {
		add( p->value );
		p = p->next;
	}
}

// ctor
template <class VType>
void JagList<VType>::copy(const JagList<VType>& list)
{
	JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );

	destroy();
	head_ = NULL;
	JagListNode<VType> *p = list.head_;
	while ( p ) {
		add( p->value );
		p = p->next;
	}
}


// 1 really deleted one record; 0 no action or not found
template <class VType>
bool JagList<VType>::remove( const VType &value, AbaxDestroyAction action )
{
	JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );

	if ( NULL == head_ ) return false;

	JagListNode<VType> *p;
	JagListNode<VType> *prev;

	if ( head_->value == value )
	{
		p = head_->next;
		if ( action != ABAX_NOOP ) {
			head_->value.destroy( action );
		}
		if ( head_ ) delete head_;
		head_ = p;
		--length_;
		return true;
	}

	prev = head_;
	p = prev->next;

	while( p != NULL )
	{
		if ( p->value == value )
		{
			prev->next = p->next;
			if ( action != ABAX_NOOP ) {
				p->value.destroy( action );
			}
			if ( p ) delete p;
			--length_;
			return true;   // delete done
		}

		prev = p;
		p=p->next;
	}

	return true;

}

template <class VType>
void JagList<VType>::print()  const
{
	bool first = true;
	for ( JagListNode<VType> *p = head_; p != NULL; p=p->next ) {
		if ( first ) {
			abaxcout << p->value; 
			first = false;
		} else {
			abaxcout << "->" << p->value; 
		}
	}

	// abaxcout << abaxendl;
}


// 0: if not found
// 1: if found old and replaced by newvalue
template <class VType>
bool JagList<VType>::replace( const VType &oldValue, const VType &newValue )
{
	JagReadWriteMutex mutex( _lock, JagReadWriteMutex::WRITE_LOCK );

	for ( JagListNode<VType> *p = head_; p != NULL; p=p->next ) {
		if ( p->value == oldValue ) {
			p->value = newValue;
			return true;
		} 
	}

	return false;
}

// 0: if not found
// 1: if found old and replaced by newvalue
template <class VType>
bool JagList<VType>::exist( const VType &value )
{
	JagReadWriteMutex mutex( _lock, JagReadWriteMutex::READ_LOCK );

	for ( JagListNode<VType> *p = head_; p != NULL; p=p->next ) {
		if ( p->value == value ) {
			return true;
		} 
	}

	return false;
}

template <class VType>
void JagList<VType>::setConcurrent( bool flag )
{
    if ( flag ) {
        if ( ! _lock ) {
            _lock = new JagReadWriteLock();
        }
    } else {
        if ( _lock ) {
            delete _lock;
        }
		_lock = NULL;
    }
}


//////////////////////// abaxlist iterator /////////////////////////////
template <class VType>
class JagListIterator
{
    public:

        JagListIterator();
        JagListIterator( JagList<VType> *list );

        ~JagListIterator( );
        void init( JagList<VType> *list );

        void begin();
        inline JagListIterator&  operator++() { nextStep(); return *this; }
        inline bool  hasNext() const { return ptr_ != NULL; }
        VType*      ptrNext();
        VType&      refNext();
        VType&      next() { VType &ref = refNext(); nextStep(); return ref; };
        void        nextStep();

    private:
        JagList<VType>     * list_;
        JagListNode<VType> *ptr_;
        bool                _doneBegin;
};


template <class VType>
JagListIterator<VType>::JagListIterator()
{
	// init( list );
}

template <class VType>
JagListIterator<VType>::JagListIterator( JagList<VType> *list )
{
	// init( list );

	this->list_ = list;
	_doneBegin = false;
	begin();
}

template <class VType>
void JagListIterator<VType>::init( JagList<VType> *list )
{
	this->list_ = list;
	_doneBegin = false;
	begin();
}

template <class VType>
JagListIterator<VType>::~JagListIterator() 
{
	_doneBegin = false;
}


template <class VType>
void JagListIterator<VType>::begin()
{
	if ( _doneBegin ) {
		return;
	}

	ptr_ = list_->head_;
	_doneBegin = true;
}



template <class VType>
void JagListIterator<VType>::nextStep()
{
	_doneBegin = false;
	ptr_ = ptr_->next;
}

template <class VType>
VType* JagListIterator<VType>::ptrNext()
{
	if ( ! ptr_ ) return NULL;
	return &(ptr_->value);
}

template <class VType>
VType& JagListIterator<VType>::refNext()
{
	if ( ! ptr_ ) {
		throw "e8374 pointer to list object is NULL"; 
		exit(1);
	}

	return ptr_->value;
}


#endif
