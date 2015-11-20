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

import org.neo4j.driver.v1.Identity;

public class IdentityValue extends ValueAdapter
{
    private final Identity val;

    public IdentityValue( Identity val )
    {
        assert val != null;
        this.val = val;
    }

    @Override
    public boolean javaBoolean()
    {
        return true;
    }

    @Override
    public String javaString()
    {
        return val.toString();
    }

    @Override
    public Identity asIdentity()
    {
        return val;
    }

    @Override
    public boolean isIdentity()
    {
        return true;
    }

    @Override
    public String toString()
    {
        return val.toString();
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

        IdentityValue values = (IdentityValue) o;

        return val.equals( values.val );

    }

    @Override
    public int hashCode()
    {
        return val.hashCode();
    }
}
