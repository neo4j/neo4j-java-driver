/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.driver.internal.value;

import org.neo4j.driver.internal.AsValue;
import org.neo4j.driver.internal.types.TypeConstructor;
import org.neo4j.driver.v1.util.Function;
import org.neo4j.driver.v1.Value;

public interface InternalValue extends Value, AsValue
{
    TypeConstructor typeConstructor();

    String toString( Format valueFormat );

    enum Format implements Function<Value, String>
    {
        VALUE_ONLY,
        VALUE_WITH_TYPE;

        public boolean includeType()
        {
            return this != VALUE_ONLY;
        }

        public Format inner()
        {
            return VALUE_ONLY;
        }

        @Override
        public String apply( Value value )
        {
            return ( (InternalValue) value ).toString( this );
        }
    }
}
