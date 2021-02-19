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
package org.neo4j.driver.internal;

import java.util.Objects;

import org.neo4j.driver.types.Point;

public class InternalPoint2D implements Point
{
    private final int srid;
    private final double x;
    private final double y;

    public InternalPoint2D( int srid, double x, double y )
    {
        this.srid = srid;
        this.x = x;
        this.y = y;
    }

    @Override
    public int srid()
    {
        return srid;
    }

    @Override
    public double x()
    {
        return x;
    }

    @Override
    public double y()
    {
        return y;
    }

    @Override
    public double z()
    {
        return Double.NaN;
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
        InternalPoint2D that = (InternalPoint2D) o;
        return srid == that.srid &&
               Double.compare( that.x, x ) == 0 &&
               Double.compare( that.y, y ) == 0;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( srid, x, y );
    }

    @Override
    public String toString()
    {
        return "Point{" +
               "srid=" + srid +
               ", x=" + x +
               ", y=" + y +
               '}';
    }
}
