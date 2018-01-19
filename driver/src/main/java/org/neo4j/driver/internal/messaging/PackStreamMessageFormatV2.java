/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.internal.messaging;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.internal.InternalCoordinate;
import org.neo4j.driver.internal.InternalPoint;
import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.packstream.PackOutput;
import org.neo4j.driver.internal.value.InternalValue;
import org.neo4j.driver.internal.value.PointValue;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Point;

import static org.neo4j.driver.internal.types.TypeConstructor.POINT_TyCon;

public class PackStreamMessageFormatV2 extends PackStreamMessageFormatV1
{
    private static final byte POINT_STRUCT_TYPE = 'X';
    private static final int POINT_STRUCT_SIZE = 3;

    @Override
    public MessageFormat.Writer newWriter( PackOutput output, boolean byteArraySupportEnabled )
    {
        if ( !byteArraySupportEnabled )
        {
            throw new IllegalArgumentException( "Bolt V2 should support byte arrays" );
        }
        return new WriterV2( output );
    }

    @Override
    public MessageFormat.Reader newReader( PackInput input )
    {
        return new ReaderV2( input );
    }

    private static class WriterV2 extends WriterV1
    {
        WriterV2( PackOutput output )
        {
            super( output, true );
        }

        @Override
        void packInternalValue( InternalValue value ) throws IOException
        {
            if ( value.typeConstructor() == POINT_TyCon )
            {
                packPoint( value.asPoint() );
            }
            else
            {
                super.packInternalValue( value );
            }
        }

        private void packPoint( Point point ) throws IOException
        {
            packer.packStructHeader( POINT_STRUCT_SIZE, POINT_STRUCT_TYPE );
            packer.pack( point.crsTableId() );
            packer.pack( point.crsCode() );
            packer.pack( point.coordinate().values() );
        }
    }

    private static class ReaderV2 extends ReaderV1
    {
        ReaderV2( PackInput input )
        {
            super( input );
        }

        @Override
        Value unpackStruct( long size, byte type ) throws IOException
        {
            if ( type == POINT_STRUCT_TYPE )
            {
                ensureCorrectStructSize( POINT_TyCon.typeName(), POINT_STRUCT_SIZE, size );
                return unpackPoint();
            }
            else
            {
                return super.unpackStruct( size, type );
            }
        }

        private Value unpackPoint() throws IOException
        {
            long crsTableId = unpacker.unpackLong();
            long crsCode = unpacker.unpackLong();

            int coordinateSize = (int) unpacker.unpackListHeader();
            List<Double> coordinate = new ArrayList<>( coordinateSize );
            for ( int i = 0; i < coordinateSize; i++ )
            {
                coordinate.add( unpacker.unpackDouble() );
            }

            Point point = new InternalPoint( crsTableId, crsCode, new InternalCoordinate( coordinate ) );
            return new PointValue( point );
        }
    }
}
