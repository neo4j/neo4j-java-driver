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

import org.neo4j.driver.v1.internal.types.TypeConstructor;

public abstract class BooleanValue extends ValueAdapter
{
    private BooleanValue()
    {
        //do nothing
    }

    @Override
    public TypeConstructor typeConstructor()
    {
        return TypeConstructor.BOOLEAN_TyCon;
    }

    public static BooleanValue TRUE = new TrueValue();

    public static BooleanValue FALSE = new FalseValue();

    public static BooleanValue fromBoolean( boolean value )
    {
        return value ? TRUE : FALSE;
    }

    @Override
    public int hashCode()
    {
        return javaBoolean() ? 1231 : 1237;
    }

    @Override
    public boolean isBoolean()
    {
        return true;
    }

    private static class TrueValue extends BooleanValue {
        @Override
        public boolean javaBoolean()
        {
            return true;
        }

        @Override
        public String javaString()
        {
            return"true";
        }
        @Override
        public int javaInteger()
        {
            return 1;
        }

        @Override
        public long javaLong()
        {
            return 1L;
        }

        @Override
        public float javaFloat()
        {
            return 1f;
        }

        @Override
        public double javaDouble()
        {
            return 1d;
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj == TRUE;
        }
    }

    private static class FalseValue extends BooleanValue {
        @Override
        public boolean javaBoolean()
        {
            return false;
        }

        @Override
        public String javaString()
        {
            return "false";
        }
        @Override
        public int javaInteger()
        {
            return 0;
        }

        @Override
        public long javaLong()
        {
            return 0L;
        }

        @Override
        public float javaFloat()
        {
            return 0f;
        }

        @Override
        public double javaDouble()
        {
            return 0d;
        }

        @Override
        public boolean equals( Object obj )
        {
            return obj == FALSE;
        }
    }
}
