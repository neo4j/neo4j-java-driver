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
package org.neo4j.driver.internal.types;

import org.neo4j.driver.internal.value.InternalValue;
import org.neo4j.driver.Value;

public enum TypeConstructor
{
    ANY
            {
                @Override
                public boolean covers( Value value )
                {
                    return !value.isNull();
                }
            },
    BOOLEAN,
    BYTES,
    STRING,
    NUMBER
            {
                @Override
                public boolean covers( Value value )
                {
                    TypeConstructor valueType = typeConstructorOf( value );
                    return valueType == this || valueType == INTEGER || valueType == FLOAT;
                }
            },
    INTEGER,
    FLOAT,
    LIST,
    MAP
            {
                @Override
                public boolean covers( Value value )
                {
                    TypeConstructor valueType = typeConstructorOf( value );
                    return valueType == MAP || valueType == NODE || valueType == RELATIONSHIP;
                }
            },
    NODE,
    RELATIONSHIP,
    PATH,
    POINT,
    DATE,
    TIME,
    LOCAL_TIME,
    LOCAL_DATE_TIME,
    DATE_TIME,
    DURATION,
    NULL;

    private static TypeConstructor typeConstructorOf( Value value )
    {
        return ((InternalValue) value).typeConstructor();
    }

    public boolean covers( Value value )
    {
        return this == typeConstructorOf( value );
    }
}
