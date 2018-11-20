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
#include <JagGlobalDef.h>

#include <abax.h>
#include <JagDBPair.h>

#include <JagGapVector.h>
#include <JagBlock.h>
#include <JagArray.h>
#include <JagSession.h>
#include <JagColumn.h>
#include <JagSchemaRecord.h>
#include <JaguarCPPClient.h>
#include <JagTableOrIndexAttrs.h>
#include <JagGeom.h>

template<> AbaxInt AbaxInt::NULLVALUE = AbaxInt(INT_MIN);
template<> AbaxLong AbaxLong::NULLVALUE = AbaxLong(LLONG_MIN);
AbaxString AbaxString::NULLVALUE = AbaxDataString();
AbaxFixString AbaxFixString::NULLVALUE = AbaxDataString();
JagDBPair JagDBPair::NULLVALUE = JagDBPair(AbaxFixString::NULLVALUE);
JagLineSeg2D JagLineSeg2D::NULLVALUE = JagLineSeg2D(LONG_MIN,LONG_MIN,LONG_MIN,LONG_MIN);
JagLineSeg2DPair JagLineSeg2DPair::NULLVALUE = JagLineSeg2DPair( JagLineSeg2D::NULLVALUE );
JagLineSeg3D JagLineSeg3D::NULLVALUE = JagLineSeg3D(LONG_MIN,LONG_MIN,LONG_MIN,LONG_MIN,LONG_MIN,LONG_MIN);
JagLineSeg3DPair JagLineSeg3DPair::NULLVALUE = JagLineSeg3DPair( JagLineSeg3D::NULLVALUE );

template<class K, class V> AbaxPair<K,V> AbaxPair<K,V>::NULLVALUE = K::NULLVALUE;

template<> AbaxPair<AbaxInt,AbaxInt> AbaxPair<AbaxInt,AbaxInt>::NULLVALUE = AbaxInt::NULLVALUE;
template<> AbaxPair<AbaxInt,AbaxBuffer> AbaxPair<AbaxInt,AbaxBuffer>::NULLVALUE = AbaxInt::NULLVALUE;
template<> AbaxPair<AbaxInt,AbaxPair<AbaxString,AbaxBuffer>> AbaxPair<AbaxInt,AbaxPair<AbaxString,AbaxBuffer>>::NULLVALUE = AbaxInt::NULLVALUE;

template<> AbaxPair<AbaxLong,AbaxInt> AbaxPair<AbaxLong,AbaxInt>::NULLVALUE = AbaxLong::NULLVALUE;
template<> AbaxPair<AbaxLong,AbaxLong> AbaxPair<AbaxLong,AbaxLong>::NULLVALUE = AbaxLong::NULLVALUE;
template<> AbaxPair<AbaxLong,AbaxLong2> AbaxPair<AbaxLong,AbaxLong2>::NULLVALUE = AbaxLong::NULLVALUE;
template<> AbaxPair<AbaxLong,AbaxDouble> AbaxPair<AbaxLong,AbaxDouble>::NULLVALUE = AbaxLong::NULLVALUE;
template<> AbaxPair<AbaxLong,AbaxString> AbaxPair<AbaxLong,AbaxString>::NULLVALUE = AbaxLong::NULLVALUE;
template<> AbaxPair<AbaxLong,AbaxChar> AbaxPair<AbaxLong,AbaxChar>::NULLVALUE = AbaxLong::NULLVALUE;
template<> AbaxPair<AbaxLong,abaxint> AbaxPair<AbaxLong,abaxint>::NULLVALUE = AbaxLong::NULLVALUE;

template<> AbaxPair<AbaxString,AbaxInt> AbaxPair<AbaxString,AbaxInt>::NULLVALUE = AbaxString::NULLVALUE;
template<> AbaxPair<AbaxString,AbaxLong> AbaxPair<AbaxString,AbaxLong>::NULLVALUE = AbaxString::NULLVALUE;
template<> AbaxPair<AbaxString,AbaxLong2> AbaxPair<AbaxString,AbaxLong2>::NULLVALUE = AbaxString::NULLVALUE;
template<> AbaxPair<AbaxString,AbaxString> AbaxPair<AbaxString,AbaxString>::NULLVALUE = AbaxString::NULLVALUE;
template<> AbaxPair<AbaxString,AbaxBuffer> AbaxPair<AbaxString,AbaxBuffer>::NULLVALUE = AbaxString::NULLVALUE;
template<> AbaxPair<AbaxString,abaxint> AbaxPair<AbaxString,abaxint>::NULLVALUE = AbaxString::NULLVALUE;
template<> AbaxPair<AbaxString,AbaxDataString> AbaxPair<AbaxString,AbaxDataString>::NULLVALUE = AbaxString::NULLVALUE;
template<> AbaxPair<AbaxString,char> AbaxPair<AbaxString,char>::NULLVALUE = AbaxString::NULLVALUE;
template<> AbaxPair<AbaxString,JagColumn> AbaxPair<AbaxString,JagColumn>::NULLVALUE = AbaxString::NULLVALUE;
template<> AbaxPair<AbaxString,JagSchemaRecord> AbaxPair<AbaxString,JagSchemaRecord>::NULLVALUE = AbaxString::NULLVALUE;
template<> AbaxPair<AbaxString,JagTableOrIndexAttrs> AbaxPair<AbaxString,JagTableOrIndexAttrs>::NULLVALUE = AbaxString::NULLVALUE;
template<> AbaxPair<AbaxString,AbaxFixString> AbaxPair<AbaxString,AbaxFixString>::NULLVALUE = AbaxString::NULLVALUE;
template<> AbaxPair<AbaxString,AbaxPair<AbaxString,AbaxBuffer>> AbaxPair<AbaxString,AbaxPair<AbaxString,AbaxBuffer>>::NULLVALUE = AbaxString::NULLVALUE;
template<> AbaxPair<AbaxString,AbaxPair<AbaxFixString,AbaxBuffer>> AbaxPair<AbaxString,AbaxPair<AbaxFixString,AbaxBuffer>>::NULLVALUE = AbaxString::NULLVALUE;
template<> AbaxPair<AbaxString,AbaxPair<AbaxString,abaxint>> AbaxPair<AbaxString,AbaxPair<AbaxString,abaxint>>::NULLVALUE = AbaxString::NULLVALUE;
template<> AbaxPair<AbaxString,AbaxPair<AbaxPair<AbaxLong,AbaxPair<AbaxLong,AbaxLong>>,AbaxPair<JagDBPair,AbaxPair<AbaxBuffer,AbaxPair<AbaxBuffer, AbaxBuffer>>>>> AbaxPair<AbaxString,AbaxPair<AbaxPair<AbaxLong,AbaxPair<AbaxLong,AbaxLong>>,AbaxPair<JagDBPair,AbaxPair<AbaxBuffer,AbaxPair<AbaxBuffer,AbaxBuffer>>>>>::NULLVALUE = AbaxString::NULLVALUE;

template<> AbaxPair<AbaxFixString,AbaxDouble> AbaxPair<AbaxFixString,AbaxDouble>::NULLVALUE = AbaxFixString::NULLVALUE;
template<> AbaxPair<AbaxFixString,AbaxFixString> AbaxPair<AbaxFixString,AbaxFixString>::NULLVALUE = AbaxFixString::NULLVALUE;
template<> AbaxPair<AbaxFixString,JagBlock<JagDBPair>*> AbaxPair<AbaxFixString,JagBlock<JagDBPair>*>::NULLVALUE = AbaxFixString::NULLVALUE;

template<> AbaxPair<JagDBPair,AbaxBuffer> AbaxPair<JagDBPair,AbaxBuffer>::NULLVALUE = JagDBPair::NULLVALUE;
template<> AbaxPair<JagDBPair,AbaxPair<AbaxInt,AbaxBuffer>> AbaxPair<JagDBPair,AbaxPair<AbaxInt,AbaxBuffer>>::NULLVALUE = JagDBPair::NULLVALUE;

