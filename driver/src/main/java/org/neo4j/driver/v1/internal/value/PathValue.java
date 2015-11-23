/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.v1.internal.value;

import org.neo4j.driver.v1.Path;

public class PathValue extends ValueAdapter
{
    private final Path adapted;

    public PathValue( Path adapted )
    {
        this.adapted = adapted;
    }

    @Override
    public Path asPath()
    {
        return adapted;
    }

    @Override
    public boolean isPath()
    {
        return true;
    }

    @Override
    public long size()
    {
        return adapted.length();
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

        PathValue values = (PathValue) o;

        return adapted.equals( values.adapted );

    }

    @Override
    public int hashCode()
    {
        return adapted.hashCode();
    }
}
