/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.driver.internal.messaging.v1;

import java.io.IOException;
import java.util.Map;

import org.neo4j.driver.internal.messaging.ValuePacker;
import org.neo4j.driver.internal.packstream.PackOutput;
import org.neo4j.driver.internal.packstream.PackStream;
import org.neo4j.driver.internal.value.InternalValue;
import org.neo4j.driver.Value;

public class ValuePackerV1 implements ValuePacker
{
    protected final PackStream.Packer packer;

    public ValuePackerV1( PackOutput output )
    {
        this.packer = new PackStream.Packer( output );
    }

    @Override
    public final void packStructHeader( int size, byte signature ) throws IOException
    {
        packer.packStructHeader( size, signature );
    }

    @Override
    public final void pack( String string ) throws IOException
    {
        packer.pack( string );
    }

    @Override
    public final void pack( Value value ) throws IOException
    {
        if ( value instanceof InternalValue )
        {
            packInternalValue( ((InternalValue) value) );
        }
        else
        {
            throw new IllegalArgumentException( "Unable to pack: " + value );
        }
    }

    @Override
    public final void pack( Map<String,Value> map ) throws IOException
    {
        if ( map == null || map.size() == 0 )
        {
            packer.packMapHeader( 0 );
            return;
        }
        packer.packMapHeader( map.size() );
        for ( Map.Entry<String,Value> entry : map.entrySet() )
        {
            packer.pack( entry.getKey() );
            pack( entry.getValue() );
        }
    }

    protected void packInternalValue( InternalValue value ) throws IOException
    {
        switch ( value.typeConstructor() )
        {
        case NULL:
            packer.packNull();
            break;

        case BYTES:
            packer.pack( value.asByteArray() );
            break;

        case STRING:
            packer.pack( value.asString() );
            break;

        case BOOLEAN:
            packer.pack( value.asBoolean() );
            break;

        case INTEGER:
            packer.pack( value.asLong() );
            break;

        case FLOAT:
            packer.pack( value.asDouble() );
            break;

        case MAP:
            packer.packMapHeader( value.size() );
            for ( String s : value.keys() )
            {
                packer.pack( s );
                pack( value.get( s ) );
            }
            break;

        case LIST:
            packer.packListHeader( value.size() );
            for ( Value item : value.values() )
            {
                pack( item );
            }
            break;

        default:
            throw new IOException( "Unknown type: " + value.type().name() );
        }
    }
}
