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

import org.neo4j.driver.internal.InternalPoint2D;
import org.neo4j.driver.internal.InternalPoint3D;
import org.neo4j.driver.internal.packstream.PackInput;
import org.neo4j.driver.internal.packstream.PackOutput;
import org.neo4j.driver.internal.value.InternalValue;
import org.neo4j.driver.internal.value.Point2DValue;
import org.neo4j.driver.internal.value.Point3DValue;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.types.Point2D;
import org.neo4j.driver.v1.types.Point3D;

import static org.neo4j.driver.internal.types.TypeConstructor.POINT_2D_TyCon;
import static org.neo4j.driver.internal.types.TypeConstructor.POINT_3D_TyCon;

public class PackStreamMessageFormatV2 extends PackStreamMessageFormatV1
{
    private static final byte POINT_2D_STRUCT_TYPE = 'X';
    private static final byte POINT_3D_STRUCT_TYPE = 'Y';
    private static final int POINT_STRUCT_SIZE = 2;

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
            if ( value.typeConstructor() == POINT_2D_TyCon )
            {
                packPoint2D( value.asPoint2D() );
            }
            else if ( value.typeConstructor() == POINT_3D_TyCon )
            {
                packPoint3D( value.asPoint3D() );
            }
            else
            {
                super.packInternalValue( value );
            }
        }

        private void packPoint2D( Point2D point ) throws IOException
        {
            packer.packStructHeader( POINT_STRUCT_SIZE, POINT_2D_STRUCT_TYPE );
            packer.pack( point.srid() );
//            packer.pack( point.x(), point.y() );
        }

        private void packPoint3D( Point3D point ) throws IOException
        {
            packer.packStructHeader( POINT_STRUCT_SIZE, POINT_3D_STRUCT_TYPE );
            packer.pack( point.srid() );
//            packer.pack( point.x(), point.y(), point.z() );
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
            if ( type == POINT_2D_STRUCT_TYPE )
            {
                ensureCorrectStructSize( POINT_2D_TyCon.typeName(), POINT_STRUCT_SIZE, size );
                return unpackPoint2D();
            }
            else if ( type == POINT_3D_STRUCT_TYPE )
            {
                ensureCorrectStructSize( POINT_3D_TyCon.typeName(), POINT_STRUCT_SIZE, size );
                return unpackPoint3D();
            }
            else
            {
                return super.unpackStruct( size, type );
            }
        }

        private Value unpackPoint2D() throws IOException
        {
            long srid = unpacker.unpackLong();
            return new Point2DValue( new InternalPoint2D( srid, 0.0, 0.0 ) );
        }

        private Value unpackPoint3D() throws IOException
        {
            long srid = unpacker.unpackLong();
            return new Point3DValue( new InternalPoint3D( srid, 0.0, 0.0, 0.0 ) );
        }
    }
}
