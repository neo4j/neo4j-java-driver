/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.driver.internal.value;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.internal.types.InternalMapAccessorWithDefaultValue;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.internal.types.TypeRepresentation;
import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.value.NotMultiValued;
import org.neo4j.driver.exceptions.value.Uncoercible;
import org.neo4j.driver.exceptions.value.Unsizable;
import org.neo4j.driver.types.Entity;
import org.neo4j.driver.types.IsoDuration;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Point;
import org.neo4j.driver.types.Relationship;
import org.neo4j.driver.types.Type;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.neo4j.driver.Values.ofObject;
import static org.neo4j.driver.Values.ofValue;

public abstract class ValueAdapter extends InternalMapAccessorWithDefaultValue implements InternalValue
{
    @Override
    public Value asValue()
    {
        return this;
    }

    @Override
    public boolean hasType( Type type )
    {
        return type.isTypeOf( this );
    }

    @Override
    public boolean isTrue()
    {
        return false;
    }

    @Override
    public boolean isFalse()
    {
        return false;
    }

    @Override
    public boolean isNull()
    {
        return false;
    }

    @Override
    public boolean containsKey( String key )
    {
        throw new NotMultiValued( type().name() + " is not a keyed collection" );
    }

    @Override
    public String asString()
    {
        throw new Uncoercible( type().name(), "Java String" );
    }

    @Override
    public boolean asBoolean( boolean defaultValue )
    {
        return computeOrDefault( Value:: asBoolean, defaultValue );
    }

    @Override
    public String asString( String defaultValue )
    {
        return computeOrDefault( (Value::asString), defaultValue );
    }

    @Override
    public long asLong( long defaultValue )
    {
        return computeOrDefault( Value::asLong, defaultValue );
    }

    @Override
    public int asInt( int defaultValue )
    {
        return computeOrDefault( Value::asInt, defaultValue );
    }

    @Override
    public double asDouble( double defaultValue )
    {
        return computeOrDefault( Value::asDouble, defaultValue );
    }

    @Override
    public float asFloat( float defaultValue )
    {
        return computeOrDefault( Value::asFloat, defaultValue );
    }

    @Override
    public long asLong()
    {
        throw new Uncoercible( type().name(), "Java long" );
    }

    @Override
    public int asInt()
    {
        throw new Uncoercible( type().name(), "Java int" );
    }

    @Override
    public float asFloat()
    {
        throw new Uncoercible( type().name(), "Java float" );
    }

    @Override
    public double asDouble()
    {
        throw new Uncoercible( type().name(), "Java double" );
    }

    @Override
    public boolean asBoolean()
    {
        throw new Uncoercible( type().name(), "Java boolean" );
    }

    @Override
    public List<Object> asList()
    {
        return asList( ofObject() );
    }

    @Override
    public <T> List<T> asList( Function<Value,T> mapFunction )
    {
        throw new Uncoercible( type().name(), "Java List" );
    }

    @Override
    public Map<String,Object> asMap()
    {
        return asMap( ofObject() );
    }

    @Override
    public <T> Map<String, T> asMap( Function<Value,T> mapFunction )
    {
        throw new Uncoercible( type().name(), "Java Map" );
    }

    @Override
    public Object asObject()
    {
        throw new Uncoercible( type().name(), "Java Object" );
    }

    @Override
    public <T> T computeOrDefault( Function<Value,T> mapper, T defaultValue )
    {
        if ( isNull() )
        {
            return defaultValue;
        }
        return mapper.apply( this );
    }

    @Override
    public Map<String,Object> asMap( Map<String,Object> defaultValue )
    {
        return computeOrDefault( Value::asMap, defaultValue );
    }

    @Override
    public <T> Map<String,T> asMap( Function<Value,T> mapFunction, Map<String,T> defaultValue )
    {
        return computeOrDefault( value -> value.asMap( mapFunction ), defaultValue );
    }

    @Override
    public byte[] asByteArray( byte[] defaultValue )
    {
        return computeOrDefault( Value::asByteArray, defaultValue );
    }

    @Override
    public List<Object> asList( List<Object> defaultValue )
    {
        return computeOrDefault( Value::asList, defaultValue );
    }

    @Override
    public <T> List<T> asList( Function<Value,T> mapFunction, List<T> defaultValue )
    {
        return computeOrDefault( value -> value.asList( mapFunction ), defaultValue );
    }

    @Override
    public LocalDate asLocalDate( LocalDate defaultValue )
    {
        return computeOrDefault( Value::asLocalDate, defaultValue );
    }

    @Override
    public OffsetTime asOffsetTime( OffsetTime defaultValue )
    {
        return computeOrDefault( Value::asOffsetTime, defaultValue );
    }

    @Override
    public LocalTime asLocalTime( LocalTime defaultValue )
    {
        return computeOrDefault( Value::asLocalTime, defaultValue );
    }

    @Override
    public LocalDateTime asLocalDateTime( LocalDateTime defaultValue )
    {
        return computeOrDefault( Value::asLocalDateTime, defaultValue );
    }

    @Override
    public OffsetDateTime asOffsetDateTime( OffsetDateTime defaultValue )
    {
        return computeOrDefault( Value::asOffsetDateTime, defaultValue );
    }

    @Override
    public ZonedDateTime asZonedDateTime( ZonedDateTime defaultValue )
    {
        return computeOrDefault( Value::asZonedDateTime, defaultValue );
    }

    @Override
    public IsoDuration asIsoDuration( IsoDuration defaultValue )
    {
        return computeOrDefault( Value::asIsoDuration, defaultValue );
    }

    @Override
    public Point asPoint( Point defaultValue )
    {
        return computeOrDefault( Value::asPoint, defaultValue );
    }

    @Override
    public byte[] asByteArray()
    {
        throw new Uncoercible( type().name(), "Byte array" );
    }

    @Override
    public Number asNumber()
    {
        throw new Uncoercible( type().name(), "Java Number" );
    }

    @Override
    public Entity asEntity()
    {
        throw new Uncoercible( type().name(), "Entity" );
    }

    @Override
    public Node asNode()
    {
        throw new Uncoercible( type().name(), "Node" );
    }

    @Override
    public Path asPath()
    {
        throw new Uncoercible( type().name(), "Path" );
    }

    @Override
    public Relationship asRelationship()
    {
        throw new Uncoercible( type().name(), "Relationship" );
    }

    @Override
    public LocalDate asLocalDate()
    {
        throw new Uncoercible( type().name(), "LocalDate" );
    }

    @Override
    public OffsetTime asOffsetTime()
    {
        throw new Uncoercible( type().name(), "OffsetTime" );
    }

    @Override
    public LocalTime asLocalTime()
    {
        throw new Uncoercible( type().name(), "LocalTime" );
    }

    @Override
    public LocalDateTime asLocalDateTime()
    {
        throw new Uncoercible( type().name(), "LocalDateTime" );
    }

    @Override
    public OffsetDateTime asOffsetDateTime()
    {
        throw new Uncoercible( type().name(), "OffsetDateTime" );
    }

    @Override
    public ZonedDateTime asZonedDateTime()
    {
        throw new Uncoercible( type().name(), "ZonedDateTime" );
    }

    @Override
    public IsoDuration asIsoDuration()
    {
        throw new Uncoercible( type().name(), "Duration" );
    }

    @Override
    public Point asPoint()
    {
        throw new Uncoercible( type().name(), "Point" );
    }

    @Override
    public Value get( int index )
    {
        throw new NotMultiValued( type().name() + " is not an indexed collection" );
    }

    @Override
    public Value get( String key )
    {
        throw new NotMultiValued( type().name() + " is not a keyed collection" );
    }

    @Override
    public int size()
    {
        throw new Unsizable( type().name() + " does not have size" );
    }

    @Override
    public Iterable<String> keys()
    {
        return emptyList();
    }

    @Override
    public boolean isEmpty()
    {
        return ! values().iterator().hasNext();
    }

    @Override
    public Iterable<Value> values()
    {
        return values( ofValue() );
    }

    @Override
    public <T> Iterable<T> values( Function<Value,T> mapFunction )
    {
        throw new NotMultiValued( type().name() + " is not iterable" );
    }

    @Override
    public final TypeConstructor typeConstructor()
    {
        return ( (TypeRepresentation) type() ).constructor();
    }

    // Force implementation
    @Override
    public abstract boolean equals( Object obj );

    // Force implementation
    @Override
    public abstract int hashCode();

    // Force implementation
    @Override
    public abstract String toString();
}


