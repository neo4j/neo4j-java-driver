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

import org.neo4j.driver.v1.types.Coordinate;
import org.neo4j.driver.v1.types.Point;

public class InternalPoint implements Point
{
    private final long crsTableId;
    private final long crsCode;
    private final Coordinate coordinate;

    public InternalPoint( long crsTableId, long crsCode, Coordinate coordinate )
    {
        this.crsTableId = crsTableId;
        this.crsCode = crsCode;
        this.coordinate = coordinate;
    }

    @Override
    public long crsTableId()
    {
        return crsTableId;
    }

    @Override
    public long crsCode()
    {
        return crsCode;
    }

    @Override
    public Coordinate coordinate()
    {
        return coordinate;
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
        InternalPoint that = (InternalPoint) o;
        return crsTableId == that.crsTableId &&
               crsCode == that.crsCode &&
               coordinate.equals( that.coordinate );
    }

    @Override
    public int hashCode()
    {
        int result = (int) (crsTableId ^ (crsTableId >>> 32));
        result = 31 * result + (int) (crsCode ^ (crsCode >>> 32));
        result = 31 * result + coordinate.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return String.format( "Point<%s, %s, %s>", crsTableId, crsCode, coordinate );
    }
}
