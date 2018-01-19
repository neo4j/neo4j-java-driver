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
package org.neo4j.driver.internal;

import java.util.List;

import org.neo4j.driver.v1.types.Coordinate;

import static java.util.Collections.unmodifiableList;

public class InternalCoordinate implements Coordinate
{
    private final List<Double> values;

    public InternalCoordinate( List<Double> values )
    {
        this.values = unmodifiableList( values );
    }

    @Override
    public List<Double> values()
    {
        return values;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        InternalCoordinate that = (InternalCoordinate) o;
        return values.equals( that.values );
    }

    @Override
    public int hashCode()
    {
        return values.hashCode();
    }

    @Override
    public String toString()
    {
        return String.format( "Coordinate<%s>", values );
    }
}
