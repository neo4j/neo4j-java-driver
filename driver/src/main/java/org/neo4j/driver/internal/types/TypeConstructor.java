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
package org.neo4j.driver.internal.types;

import org.neo4j.driver.internal.value.InternalValue;
import org.neo4j.driver.v1.Value;

public enum TypeConstructor
{
    ANY_TyCon {
        @Override
        public String typeName()
        {
            return "ANY";
        }

        @Override
        public boolean covers( Value value )
        {
            return ! value.isNull();
        }
    },
    BOOLEAN_TyCon {
        @Override
        public String typeName()
        {
            return "BOOLEAN";
        }
    },

    STRING_TyCon {
        @Override
        public String typeName()
        {
            return "STRING";
        }
    },

    NUMBER_TyCon {
        @Override
        public boolean covers( Value value )
        {
            TypeConstructor valueType = typeConstructorOf( value );
            return valueType == this || valueType == INTEGER_TyCon || valueType == FLOAT_TyCon;
        }

        @Override
        public String typeName()
        {
            return "NUMBER";
        }
    },

    INTEGER_TyCon {
        @Override
        public String typeName()
        {
            return "INTEGER";
        }
    },

    FLOAT_TyCon {
        @Override
        public String typeName()
        {
            return "FLOAT";
        }
    },

    LIST_TyCon {
        @Override
        public String typeName()
        {
            return "LIST";
        }
    },

    MAP_TyCon {
        @Override
        public String typeName()
        {
            return "MAP";
        }

        @Override
        public boolean covers( Value value )
        {
            TypeConstructor valueType = typeConstructorOf( value );
            return valueType == MAP_TyCon || valueType == NODE_TyCon || valueType == RELATIONSHIP_TyCon;
        }
    },

    NODE_TyCon {
        @Override
        public String typeName()
        {
            return "NODE";
        }
    },

    RELATIONSHIP_TyCon
            {
        @Override
        public String typeName()
        {
            return "RELATIONSHIP";
        }
    },

    PATH_TyCon {
        @Override
        public String typeName()
        {
            return "PATH";
        }
    },

    NULL_TyCon {
        @Override
        public String typeName()
        {
            return "NULL";
        }
    };

    private static TypeConstructor typeConstructorOf( Value value )
    {
        return ( (InternalValue) value ).typeConstructor();
    }

    public abstract String typeName();

    public boolean covers( Value value )
    {
        return this == typeConstructorOf( value );
    }
}
